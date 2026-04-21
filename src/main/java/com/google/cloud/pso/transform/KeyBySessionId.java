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

import com.google.cloud.pso.model.Event;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** A PTransform that keys events by their session ID. */
public class KeyBySessionId extends PTransform<PCollection<Event>, PCollection<KV<String, Event>>> {

    private KeyBySessionId() {}

    public static KeyBySessionId of() {
        return new KeyBySessionId();
    }

    @Override
    public PCollection<KV<String, Event>> expand(PCollection<Event> input) {
        return input.apply(ParDo.of(new KeyBySessionIdFn()));
    }

    private static class KeyBySessionIdFn extends DoFn<Event, KV<String, Event>> {
        @ProcessElement
        public void processElement(
                @Element Event event, OutputReceiver<KV<String, Event>> receiver) {
            receiver.output(KV.of(event.getSessionId(), event));
        }
    }
}
