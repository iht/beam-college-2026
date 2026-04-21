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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pso.model.Event;
import com.google.cloud.pso.model.Order;
import com.google.cloud.pso.storage.StateStore;
import com.google.cloud.pso.storage.StateStoreProvider;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MergeFnTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static class FakeStateStoreProvider implements StateStoreProvider {
        private final String providerId;
        private static final Map<String, StateStore> mocks = new ConcurrentHashMap<>();

        FakeStateStoreProvider(StateStore stateStore) {
            this.providerId = UUID.randomUUID().toString();
            mocks.put(providerId, stateStore);
        }

        @Override
        public StateStore getStateStore(String baseDir) {
            return mocks.get(providerId);
        }
    }

    @Test
    public void testMergeFnOutputsOrder() {
        String sessionId = "session-1";
        Map<String, Object> data = new HashMap<>();
        data.put("item_id", "p1");
        data.put("quantity", 2);
        Event event = new Event(sessionId, 100L, "ADD_TO_CART", data);

        StateStore mockStateStore = mock(StateStore.class);

        PCollectionTuple results =
                pipeline.apply(Create.of(KV.of(sessionId, event)))
                        .apply(
                                "MergeEvents",
                                MergeFn.of(
                                        "/tmp/state",
                                        null,
                                        new FakeStateStoreProvider(mockStateStore)));

        PCollection<Order> orders = results.get(MergeFn.SUCCESS_TAG);

        PAssert.that(orders)
                .satisfies(
                        elements -> {
                            Order order = elements.iterator().next();
                            if (!order.getSessionId().equals(sessionId)) {
                                throw new AssertionError("Order sessionId mismatch");
                            }
                            if (order.getItems().get("p1") != 2) {
                                throw new AssertionError("Order items quantity mismatch");
                            }
                            return null;
                        });

        pipeline.run();
    }

    @Test
    public void testEventTimeTimerIsSet() throws Exception {
        String sessionId = "session-timer";
        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 1);
        Event e1 = new Event(sessionId, 1000L, "ADD_TO_CART", data1);

        StateStore mockStateStore = mock(StateStore.class);

        TestStream<KV<String, Event>> stream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        .advanceWatermarkTo(Instant.ofEpochMilli(1000L))
                        .addElements(KV.of(sessionId, e1))
                        .advanceWatermarkToInfinity();

        PCollectionTuple results =
                pipeline.apply(stream)
                        .apply(
                                MergeFn.of(
                                        "/tmp/state",
                                        2,
                                        new FakeStateStoreProvider(mockStateStore)));

        PAssert.that(results.get(MergeFn.SUCCESS_TAG)).satisfies(it -> null);

        pipeline.run();

        // Verify that update was called
        verify(mockStateStore, times(1)).update(anyString(), anyString());
    }

    @Test
    public void testEventTimeTimerResets() throws Exception {
        String sessionId = "session-timer-reset";
        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 1);
        Event e1 = new Event(sessionId, 1000L, "ADD_TO_CART", data1);

        Map<String, Object> data2 = new HashMap<>();
        data2.put("item_id", "p2");
        data2.put("quantity", 1);
        Event e2 = new Event(sessionId, 2000L, "ADD_TO_CART", data2);

        StateStore mockStateStore = mock(StateStore.class);

        TestStream<KV<String, Event>> stream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        .advanceWatermarkTo(Instant.ofEpochMilli(1000L))
                        .addElements(KV.of(sessionId, e1))
                        // Timer set to 1000 + 2000 = 3000ms
                        .advanceWatermarkTo(Instant.ofEpochMilli(2000L))
                        .addElements(KV.of(sessionId, e2))
                        // Timer reset to 2000 + 2000 = 4000ms
                        .advanceWatermarkTo(Instant.ofEpochMilli(3500L))
                        // Watermark is past 3000ms, but timer was reset to 4000ms, so shouldn't
                        // fire
                        // yet
                        .advanceWatermarkTo(Instant.ofEpochMilli(4500L))
                        // Watermark is past 4000ms, should fire now
                        .advanceWatermarkToInfinity();

        PCollectionTuple results =
                pipeline.apply(stream)
                        .apply(
                                MergeFn.of(
                                        "/tmp/state",
                                        2,
                                        new FakeStateStoreProvider(mockStateStore)));

        PAssert.that(results.get(MergeFn.SUCCESS_TAG)).satisfies(it -> null);

        pipeline.run();

        // Verify that update was called exactly once (when the timer finally fired)
        verify(mockStateStore, times(1)).update(anyString(), anyString());
    }

    @Test
    public void testStateMigrationFailureRetainsState() throws Exception {
        String sessionId = "session-fail";
        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 1);
        Event e1 = new Event(sessionId, 1000L, "ADD_TO_CART", data1);

        StateStore mockStateStore = mock(StateStore.class);
        // Throw exception on first call, succeed on second
        doThrow(new IOException("Transient failure"))
                .doNothing()
                .when(mockStateStore)
                .update(anyString(), anyString());

        TestStream<KV<String, Event>> stream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        .advanceWatermarkTo(Instant.ofEpochMilli(1000L))
                        .addElements(KV.of(sessionId, e1))
                        // First trigger at 3000ms (will fail and reschedule for 5000ms)
                        .advanceWatermarkTo(Instant.ofEpochMilli(4000L))
                        // Second trigger at 5000ms (will succeed)
                        .advanceWatermarkTo(Instant.ofEpochMilli(6000L))
                        .advanceWatermarkToInfinity();

        PCollectionTuple results =
                pipeline.apply(stream)
                        .apply(
                                MergeFn.of(
                                        "/tmp/state",
                                        2,
                                        new FakeStateStoreProvider(mockStateStore)));

        PAssert.that(results.get(MergeFn.SUCCESS_TAG)).satisfies(it -> null);

        pipeline.run();

        // Verify that update was called exactly twice (failed once, succeeded once)
        verify(mockStateStore, times(2)).update(anyString(), anyString());
    }

    @Test
    public void testStateWarmUpFromExternal() throws Exception {
        String sessionId = "session-warmup";
        Order existingOrder = new Order(sessionId);
        // We must add an event because Order.recalculate() (called by addEvent)
        // wipes items and reapplies events.
        Map<String, Object> dataExisting = new HashMap<>();
        dataExisting.put("item_id", "p_existing");
        dataExisting.put("quantity", 5);
        existingOrder.addEvent(new Event(sessionId, 500L, "ADD_TO_CART", dataExisting));

        String existingStateJson = OBJECT_MAPPER.writeValueAsString(existingOrder);

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p_new");
        data1.put("quantity", 1);
        Event newEvent = new Event(sessionId, 1000L, "ADD_TO_CART", data1);

        StateStore mockStateStore = mock(StateStore.class);
        when(mockStateStore.read(sessionId)).thenReturn(existingStateJson);

        PCollectionTuple results =
                pipeline.apply(Create.of(KV.of(sessionId, newEvent)))
                        .apply(
                                MergeFn.of(
                                        "/tmp/state",
                                        2,
                                        new FakeStateStoreProvider(mockStateStore)));

        PAssert.that(results.get(MergeFn.SUCCESS_TAG))
                .satisfies(
                        elements -> {
                            Order order = elements.iterator().next();
                            if (order.getItems().get("p_existing") != 5) {
                                throw new AssertionError("Existing items missing after warmup");
                            }
                            if (order.getItems().get("p_new") != 1) {
                                throw new AssertionError("New item not merged after warmup");
                            }
                            return null;
                        });

        pipeline.run();
    }
}
