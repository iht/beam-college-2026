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
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple DoFn to parse JSON strings with error handling. */
public class ParseEventFn extends DoFn<String, Event> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TupleTag<Event> successTag;
    private final TupleTag<String> failureTag;

    public ParseEventFn(TupleTag<Event> successTag, TupleTag<String> failureTag) {
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(@Element String json, MultiOutputReceiver receiver) {
        try {
            Event event = OBJECT_MAPPER.readValue(json, Event.class);
            receiver.get(successTag).output(event);
        } catch (Exception e) {
            LOG.error("Failed to parse event JSON: " + json, e);
            receiver.get(failureTag).output(json);
        }
    }
}
