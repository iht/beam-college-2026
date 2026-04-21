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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform that wraps a stateful DoFn to merge shopping events into an aggregated Order state.
 * It uses a two-level cache: Beam state (first level) and a persistent StateStore (second level).
 */
public class MergeFn extends PTransform<PCollection<KV<String, Event>>, PCollectionTuple> {

    public static final TupleTag<Order> SUCCESS_TAG = new TupleTag<Order>() {};
    public static final TupleTag<String> FAILURE_TAG = new TupleTag<String>() {};

    private final String stateBaseDir;
    private final Integer stateMoveThresholdSeconds;
    private final StateStoreProvider stateStoreProvider;

    private MergeFn(
            String stateBaseDir,
            Integer stateMoveThresholdSeconds,
            StateStoreProvider stateStoreProvider) {
        this.stateBaseDir = stateBaseDir;
        this.stateMoveThresholdSeconds = stateMoveThresholdSeconds;
        this.stateStoreProvider = stateStoreProvider;
    }

    public static MergeFn of(
            String stateBaseDir,
            Integer stateMoveThresholdSeconds,
            StateStoreProvider stateStoreProvider) {
        return new MergeFn(stateBaseDir, stateMoveThresholdSeconds, stateStoreProvider);
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<String, Event>> input) {
        return input.apply(
                ParDo.of(new MergeDoFn(stateBaseDir, stateMoveThresholdSeconds, stateStoreProvider))
                        .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
    }

    private static class MergeDoFn extends DoFn<KV<String, Event>, Order> {

        private static final Logger LOG = LoggerFactory.getLogger(MergeDoFn.class);
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private final String stateBaseDir;
        private final int stateMoveThresholdSeconds;
        private final StateStoreProvider stateStoreProvider;
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

        public MergeDoFn(
                String stateBaseDir,
                Integer stateMoveThresholdSeconds,
                StateStoreProvider stateStoreProvider) {
            this.stateBaseDir = stateBaseDir;
            this.stateMoveThresholdSeconds =
                    stateMoveThresholdSeconds != null ? stateMoveThresholdSeconds : 2;
            this.stateStoreProvider = stateStoreProvider;
        }

        @Setup
        @SuppressWarnings("unused")
        public void setup() {
            this.stateStore = stateStoreProvider.getStateStore(stateBaseDir);
        }

        @Teardown
        @SuppressWarnings("unused")
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
                Order order = state.read();

                if (order == null) {
                    order = lookupExternalState(sessionId);
                    if (order == null) {
                        order = new Order(sessionId);
                    } else {
                        state.write(order);
                    }
                }

                order.addEvent(newEvent);
                state.write(order);
                receiver.get(SUCCESS_TAG).output(order);
                timer.offset(Duration.standardSeconds(stateMoveThresholdSeconds)).setRelative();
            } catch (Exception e) {
                LOG.error("Failed to process event for session: " + sessionId, e);
                try {
                    receiver.get(FAILURE_TAG).output(OBJECT_MAPPER.writeValueAsString(newEvent));
                } catch (IOException ioException) {
                    LOG.error("Failed to write failed event to DLQ", ioException);
                }
            }
        }

        @OnTimer("stateTimer")
        @SuppressWarnings("unused")
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
                                "Moved state to external storage for session: "
                                        + order.getSessionId());
                        state.clear();
                    } catch (IOException e) {
                        LOG.error(
                                "Failed to move state to external storage for session: "
                                        + order.getSessionId()
                                        + ". Will retry.",
                                e);
                        timer.set(
                                c.timestamp()
                                        .plus(Duration.standardSeconds(stateMoveThresholdSeconds)));
                    }
                } else {
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
}
