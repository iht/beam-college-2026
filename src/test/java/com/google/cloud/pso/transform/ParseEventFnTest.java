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
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParseEventFnTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    public static final TupleTag<Event> SUCCESS_TAG = new TupleTag<Event>() {};
    public static final TupleTag<String> FAILURE_TAG = new TupleTag<String>() {};

    @Test
    public void testParseValidJson() throws Exception {
        String validJson =
                "{\"session_id\":\"s1\", \"timestamp\":100, \"event_type\":\"ADD_TO_CART\","
                        + " \"data\":{\"item_id\":\"p1\", \"quantity\":1}}";
        PCollection<String> input = pipeline.apply(Create.of(validJson));

        PCollectionTuple output = input.apply(ParseEventFn.of(SUCCESS_TAG, FAILURE_TAG));

        Event expectedEvent = new ObjectMapper().readValue(validJson, Event.class);
        PAssert.that(output.get(SUCCESS_TAG)).containsInAnyOrder(expectedEvent);
        PAssert.that(output.get(FAILURE_TAG)).empty();

        pipeline.run();
    }

    @Test
    public void testParseInvalidJson() {
        String invalidJson = "invalid-json";
        PCollection<String> input = pipeline.apply(Create.of(invalidJson));

        PCollectionTuple output = input.apply(ParseEventFn.of(SUCCESS_TAG, FAILURE_TAG));

        PAssert.that(output.get(SUCCESS_TAG)).empty();
        PAssert.that(output.get(FAILURE_TAG)).containsInAnyOrder(invalidJson);

        pipeline.run();
    }
}
