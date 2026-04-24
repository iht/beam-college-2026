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

import com.google.cloud.pso.extract.CreateEvents;
import com.google.cloud.pso.model.Event;
import com.google.cloud.pso.model.Order;
import com.google.cloud.pso.options.SessionMergeOptions;
import com.google.cloud.pso.storage.DefaultStateStoreProvider;
import com.google.cloud.pso.transform.KeyBySessionId;
import com.google.cloud.pso.transform.MergeFn;
import com.google.cloud.pso.transform.ParseEventFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating the session merge pipeline. It generates events internally, merges
 * them using stateful processing, and outputs the aggregated state to stdout.
 */
public class PipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineFactory.class);

    public static Pipeline createPipeline(SessionMergeOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // 1. Internal Event Generation
        PCollection<String> rawEvents =
                pipeline.apply("GenerateEvents", CreateEvents.of(options.getNumEvents()));

        // 2. Parse JSON strings into Event objects
        PCollectionTuple parsedEvents = rawEvents.apply("ParseEvents", ParseEventFn.of());

        // 3. Key events by SessionId
        PCollection<KV<String, Event>> keyedEvents =
                parsedEvents
                        .get(ParseEventFn.SUCCESS_TAG)
                        .apply("KeyBySessionId", KeyBySessionId.of());

        // 4. Stateful Process and Merge
        PCollectionTuple mergeResult =
                keyedEvents.apply(
                        "ProcessAndMerge",
                        MergeFn.of(
                                options.getStateBaseDir(),
                                options.getStateMoveThresholdSeconds(),
                                new DefaultStateStoreProvider()));

        PCollection<Order> mergedSessions = mergeResult.get(MergeFn.SUCCESS_TAG);
        PCollection<String> mergeFailures = mergeResult.get(MergeFn.FAILURE_TAG);

        // 5. Handle All Failures (Parsing + Merging)
        PCollectionList.of(parsedEvents.get(ParseEventFn.FAILURE_TAG))
                .and(mergeFailures)
                .apply("FlattenFailures", Flatten.pCollections())
                .apply(
                        "LogFailures",
                        ParDo.of(
                                new DoFn<String, Void>() {
                                    @ProcessElement
                                    public void processElement(@Element String element) {
                                        LOG.error("Failure output: " + element);
                                    }
                                }));

        // 6. Write successful merges to stdout
        mergedSessions.apply(
                "WriteToStdout",
                ParDo.of(
                        new DoFn<Order, Void>() {
                            @ProcessElement
                            public void processElement(@Element Order element) {
                                System.out.println(element);
                            }
                        }));

        return pipeline;
    }
}
