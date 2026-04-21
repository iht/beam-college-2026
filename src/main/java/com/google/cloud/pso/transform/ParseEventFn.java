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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple PTransform to parse JSON strings with error handling. */
public class ParseEventFn extends PTransform<PCollection<String>, PCollectionTuple> {

    public static final TupleTag<Event> SUCCESS_TAG = new TupleTag<Event>() {};
    public static final TupleTag<String> FAILURE_TAG = new TupleTag<String>() {};

    private ParseEventFn() {}

    public static ParseEventFn of() {
        return new ParseEventFn();
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
        return input.apply(
                ParDo.of(new ParseDoFn())
                        .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
    }

    private static class ParseDoFn extends DoFn<String, Event> {
        private static final Logger LOG = LoggerFactory.getLogger(ParseDoFn.class);
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        public ParseDoFn() {}

        @ProcessElement
        public void processElement(@Element String json, MultiOutputReceiver receiver) {
            try {
                Event event = OBJECT_MAPPER.readValue(json, Event.class);
                receiver.get(SUCCESS_TAG).output(event);
            } catch (Exception e) {
                LOG.error("Failed to parse event JSON: " + json, e);
                receiver.get(FAILURE_TAG).output(json);
            }
        }
    }
}
