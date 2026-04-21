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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pso.model.Event;
import com.google.cloud.pso.model.Order;
import com.google.cloud.pso.storage.StateStore;
import com.google.cloud.pso.storage.StateStoreProvider;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MergeFnStateStoreTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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
    public void testMergeLogicWithStateStoreLookup() throws Exception {
        String sessionId = "session-1";

        Order existingOrder = new Order(sessionId);
        Map<String, Object> data0 = new HashMap<>();
        data0.put("item_id", "p0");
        data0.put("quantity", 5);
        existingOrder.addEvent(new Event(sessionId, 50L, "ADD_TO_CART", data0));

        String existingStateJson =
                new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(existingOrder);

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 1);
        Event e1 = new Event(sessionId, 100L, "ADD_TO_CART", data1);

        StateStore mockStateStore = mock(StateStore.class);
        when(mockStateStore.read(sessionId)).thenReturn(existingStateJson);

        PCollectionTuple output =
                pipeline.apply(Create.of(KV.of(sessionId, e1)))
                        .apply(
                                MergeFn.of(
                                        "/tmp/state",
                                        null,
                                        new FakeStateStoreProvider(mockStateStore)));

        PAssert.that(output.get(MergeFn.SUCCESS_TAG))
                .satisfies(
                        elements -> {
                            Order order = elements.iterator().next();
                            String element = order.toString();
                            if (!element.contains("p0") || !element.contains("p1")) {
                                throw new AssertionError(
                                        "Output should contain both p0 (from StateStore) and p1"
                                                + " (from event). Got: "
                                                + element);
                            }
                            return null;
                        });

        pipeline.run();
    }

    @Test
    public void testTimerPersistenceToStateStore() throws Exception {
        String sessionId = "session-timer";
        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 1);
        Event e1 = new Event(sessionId, 100L, "ADD_TO_CART", data1);

        StateStore mockStateStore = mock(StateStore.class);
        when(mockStateStore.read(sessionId)).thenReturn(null);

        TestStream<KV<String, Event>> stream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        .addElements(KV.of(sessionId, e1))
                        .advanceProcessingTime(Duration.standardSeconds(65))
                        .advanceWatermarkToInfinity();

        pipeline.apply(stream)
                .apply(MergeFn.of("/tmp/state", null, new FakeStateStoreProvider(mockStateStore)));

        pipeline.run();

        // Verify that update was called when timer fired
        verify(mockStateStore).update(anyString(), anyString());
    }
}
