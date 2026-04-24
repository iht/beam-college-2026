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

package com.google.cloud.pso.extract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pso.model.Event;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineExtension;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the CreateEvents transform. */
@ExtendWith(TestPipelineExtension.class)
public class CreateEventsTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testCreateEventsCount(TestPipeline pipeline) {
        int numEvents = 100;
        PCollection<String> events = pipeline.apply(CreateEvents.of(numEvents));

        PAssert.thatSingleton(events.apply("CountEvents", Count.globally()))
                .isEqualTo((long) numEvents);

        pipeline.run();
    }

    @Test
    public void testCreateEventsSerialization(TestPipeline pipeline) {
        int numEvents = 10;
        PCollection<String> events = pipeline.apply(CreateEvents.of(numEvents));

        // Verify we can deserialize back to Event objects
        events.apply("VerifyDeserialization", ParDo.of(new VerifyDeserializationFn()));

        pipeline.run();
    }

    private static class VerifyDeserializationFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String json) throws Exception {
            OBJECT_MAPPER.readValue(json, Event.class);
        }
    }
}
