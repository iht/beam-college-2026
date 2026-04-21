/*
*  Copyright 2026 Google LLC
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      https://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package com.google.cloud.pso.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pso.model.Event;
import com.google.cloud.pso.model.Order;
import com.google.cloud.pso.storage.StateStore;
import com.google.cloud.pso.storage.StateStoreProvider;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stateful DoFn that merges shopping events into an aggregated Order state. It uses a two-level
 * cache: Beam state (first level) and a persistent StateStore (second level).
 */
public class MergeFn extends DoFn<KV<String, Event>, Order> {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFn.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String stateBaseDir;
    private final int stateMoveThresholdSeconds;
    private final StateStoreProvider stateStoreProvider;
    private final TupleTag<Order> successTag;
    private final TupleTag<String> failureTag;
    public transient StateStore stateStore;

    private static Coder<Order> getOrderCoder() {
        try {
            return SchemaRegistry.createDefault().getSchemaCoder(Order.class);
        } catch (NoSuchSchemaException e) {
            throw new RuntimeException(e);
        }
    }

    @StateId("accumulatedEvents")
    private final StateSpec<ValueState<Order>> accumulatedEventsSpec =
            StateSpecs.value(getOrderCoder());

    @TimerId("stateTimer")
    private final TimerSpec stateTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    public MergeFn(
            String stateBaseDir,
            Integer stateMoveThresholdSeconds,
            StateStoreProvider stateStoreProvider,
            TupleTag<Order> successTag,
            TupleTag<String> failureTag) {
        this.stateBaseDir = stateBaseDir;
        this.stateMoveThresholdSeconds =
                stateMoveThresholdSeconds != null ? stateMoveThresholdSeconds : 2;
        this.stateStoreProvider = stateStoreProvider;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @Setup
    public void setup() {
        this.stateStore = stateStoreProvider.getStateStore(stateBaseDir);
    }

    @Teardown
    public void teardown() throws Exception {
        if (this.stateStore != null) {
            this.stateStore.close();
        }
    }

    @ProcessElement
    public void processElement(
            @Element KV<String, Event> element,
            MultiOutputReceiver receiver,
            @StateId("accumulatedEvents") ValueState<Order> state,
            @TimerId("stateTimer") Timer timer) {

        String sessionId = element.getKey();
        Event newEvent = element.getValue();

        try {
            // 1. Read from in-memory state
            Order order = state.read();

            if (order == null) {
                // 2. Lookup external state if not in memory
                order = lookupExternalState(sessionId);
                if (order == null) {
                    order = new Order(sessionId);
                } else {
                    // WARM UP: If we found external state, write it to internal state
                    // so subsequent elements can use it without another lookup.
                    state.write(order);
                }
            }

            // 3. Add new event and recalculate state
            order.addEvent(newEvent);

            // 4. Update state
            state.write(order);

            // 5. Output merged session as Order object
            receiver.get(successTag).output(order);

            // 6. Reset timer
            timer.offset(Duration.standardSeconds(stateMoveThresholdSeconds)).setRelative();
        } catch (Exception e) {
            LOG.error("Failed to process event for session: " + sessionId, e);
            try {
                receiver.get(failureTag).output(OBJECT_MAPPER.writeValueAsString(newEvent));
            } catch (IOException ioException) {
                LOG.error("Failed to write failed event to DLQ", ioException);
            }
        }
    }

    @OnTimer("stateTimer")
    public void onTimer(
            OnTimerContext c,
            @StateId("accumulatedEvents") ValueState<Order> state,
            @TimerId("stateTimer") Timer timer)
            throws IOException {

        LOG.info("Timer fired for processing state.");
        Order order = state.read();
        if (order != null) {
            if (order.getSessionId() != null) {
                try {
                    writeToExternalState(
                            order.getSessionId(), OBJECT_MAPPER.writeValueAsString(order));
                    LOG.info(
                            "Moved state to external storage for session: " + order.getSessionId());
                    // Clear state ONLY on success
                    state.clear();
                } catch (IOException e) {
                    LOG.error(
                            "Failed to move state to external storage for session: "
                                    + order.getSessionId()
                                    + ". Will retry.",
                            e);
                    // Reset timer to retry (using current trigger time + threshold)
                    timer.set(
                            c.timestamp()
                                    .plus(Duration.standardSeconds(stateMoveThresholdSeconds)));
                }
            } else {
                // Should not happen, but clear to be safe
                state.clear();
            }
        } else {
            LOG.warn("Timer fired but state was empty.");
        }
    }

    private Order lookupExternalState(String sessionId) throws IOException {
        String data = stateStore.read(sessionId);
        if (data != null) {
            return OBJECT_MAPPER.readValue(data, Order.class);
        }
        return null;
    }

    private void writeToExternalState(String sessionId, String dataJson) throws IOException {
        stateStore.update(sessionId, dataJson);
    }
}
