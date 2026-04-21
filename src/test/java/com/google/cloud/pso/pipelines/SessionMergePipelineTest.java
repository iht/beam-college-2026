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

package com.google.cloud.pso.pipelines;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.pso.model.Event;
import com.google.cloud.pso.model.Order;
import com.google.cloud.pso.options.SessionMergeOptions;
import com.google.cloud.pso.storage.StateStore;
import com.google.cloud.pso.storage.StateStoreProvider;
import com.google.cloud.pso.transform.MergeFn;
import com.google.cloud.pso.transform.ParseEventFn;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SessionMergePipelineTest {

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

    private static class KeyBySessionIdFn extends DoFn<Event, KV<String, Event>> {
        private final String sessionId;

        KeyBySessionIdFn(String sessionId) {
            this.sessionId = sessionId;
        }

        @ProcessElement
        public void processElement(
                @Element Event event, OutputReceiver<KV<String, Event>> receiver) {
            receiver.output(KV.of(sessionId, event));
        }
    }

    @Test
    public void testFullPipelineLogic() throws Exception {
        String sessionId = "session-test";
        String validJson =
                "{\"session_id\":\""
                        + sessionId
                        + "\", \"timestamp\":100, \"event_type\":\"ADD_TO_CART\","
                        + " \"data\":{\"item_id\":\"p1\", \"quantity\":1}}";
        String invalidJson = "invalid-json";

        StateStore mockStateStore = mock(StateStore.class);
        when(mockStateStore.read(anyString())).thenReturn(null);

        SessionMergeOptions options = PipelineOptionsFactory.as(SessionMergeOptions.class);
        options.setStateBaseDir("/tmp");

        PCollection<String> input =
                pipeline.apply(Create.of(Arrays.asList(validJson, invalidJson)));

        // Test Parsing and DLQ logic
        PCollectionTuple parsedEvents = input.apply("ParseEvents", ParseEventFn.of());

        PAssert.that(parsedEvents.get(ParseEventFn.FAILURE_TAG)).containsInAnyOrder(invalidJson);

        // Test Integration with MergeFn
        PCollectionTuple output =
                parsedEvents
                        .get(ParseEventFn.SUCCESS_TAG)
                        .apply("KeyBySessionId", ParDo.of(new KeyBySessionIdFn(sessionId)))
                        .apply(
                                "ProcessAndMerge",
                                MergeFn.of(
                                        "/tmp", null, new FakeStateStoreProvider(mockStateStore)));

        PAssert.that(output.get(MergeFn.SUCCESS_TAG))
                .satisfies(
                        elements -> {
                            Order order = elements.iterator().next();
                            String element = order.toString();
                            if (!element.contains("p1") || !element.contains(sessionId)) {
                                throw new AssertionError(
                                        "Output should contain p1 and sessionId. Got: " + element);
                            }
                            return null;
                        });

        pipeline.run();
    }
}
