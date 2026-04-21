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

package com.google.cloud.pso;

import com.google.cloud.pso.model.Event;
import com.google.cloud.pso.model.Order;
import com.google.cloud.pso.storage.DefaultStateStoreProvider;
import com.google.cloud.pso.transform.MergeFn;
import java.io.File;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * BeamCollegeDemoTest is a comprehensive end-to-end unit test designed for step-by-step debugging
 * and demonstration of the SessionMergePipeline logic.
 *
 * <p>It demonstrates:
 *
 * <ul>
 *   <li>Merging of order fragments in Apache Beam's internal state.
 *   <li>Automatic offloading of state to a cold file-based cache when a session becomes inactive.
 *   <li>Warming up state from the cold cache when a late-arriving fragment for an offloaded session
 *       is processed.
 * </ul>
 *
 * <p>To use this for a demo:
 *
 * <ol>
 *   <li>Open {@code com.google.cloud.pso.transform.MergeFn}.
 *   <li>Set breakpoints in {@code processElement} and {@code onTimer}.
 *   <li>Run this test in Debug mode.
 * </ol>
 */
@RunWith(JUnit4.class)
public class BeamCollegeDemoTest implements Serializable {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();
    @Rule public final transient TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Demonstrates merging fragments for a single order and triggering the offloading timer.
     *
     * <p>Flow:
     *
     * <ol>
     *   <li>Fragment 1 arrives at T=0. Internal state is created.
     *   <li>Fragment 2 arrives at T=1s. Merged into internal state.
     *   <li>Watermark advances to T=10s. The 2-second inactivity timer fires, moving state to the
     *       cold cache.
     *   <li>Fragment 3 arrives at T=11s. State is warmed up from cold cache and merged.
     * </ol>
     */
    @Test
    public void testSingleOrderMergingAndOffloading() throws Exception {
        String sessionId = "demo-session-1";
        Instant start = Instant.ofEpochMilli(0);
        File stateDir = tempFolder.newFolder("state");
        String stateBaseDir = stateDir.getAbsolutePath();

        List<Event> events = DemoTestUtils.createSingleOrderScenario(sessionId, start);

        TestStream<KV<String, Event>> testStream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        .addElements(TimestampedValue.of(KV.of(sessionId, events.get(0)), start))
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionId, events.get(1)),
                                        start.plus(Duration.standardSeconds(1))))
                        .advanceWatermarkTo(start.plus(Duration.standardSeconds(10)))
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionId, events.get(2)),
                                        start.plus(Duration.standardSeconds(11))))
                        .advanceWatermarkToInfinity();

        PCollectionTuple results =
                pipeline.apply(testStream)
                        .apply(MergeFn.of(stateBaseDir, 2, new DefaultStateStoreProvider()));

        PCollection<Order> successfulOrders = results.get(MergeFn.SUCCESS_TAG);

        PAssert.that(successfulOrders)
                .satisfies(
                        orders -> {
                            int count = 0;
                            for (Order _ : orders) {
                                count++;
                            }
                            // Expected outputs:
                            // 1. After fragment 1 (internal state)
                            // 2. After fragment 2 (internal state)
                            // 3. After fragment 3 (warmed up from cold cache + fragment 3)
                            Assert.assertTrue("Should have at least 3 outputs", count >= 3);
                            return null;
                        });

        pipeline.run().waitUntilFinish();

        // Verify that the state file was created at some point (it might have been deleted if
        // another fragment arrived,
        // depending on how MergeFn handles warm up - actually MergeFn WARM UP writes back to
        // internal state but does it delete from external?
        // Let's check MergeFn.java)
        // From MergeFn.java: lookupExternalState reads it. It doesn't seem to delete it
        // immediately.
        Assert.assertTrue(
                "State file should exist in cold cache",
                DemoTestUtils.checkStateFileExists(stateBaseDir, sessionId));
    }

    /**
     * Demonstrates that the pipeline correctly separates and matches interleaved fragments from
     * different orders arriving in the same stream.
     */
    @Test
    public void testInterleavedOrdersMatching() throws Exception {
        String sessionIdA = "session-A";
        String sessionIdB = "session-B";
        Instant start = Instant.ofEpochMilli(0);
        File stateDir = tempFolder.newFolder("state-interleaved");
        String stateBaseDir = stateDir.getAbsolutePath();

        List<Event> events =
                DemoTestUtils.createInterleavedOrdersScenario(sessionIdA, sessionIdB, start);

        TestStream<KV<String, Event>> testStream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        .addElements(TimestampedValue.of(KV.of(sessionIdA, events.get(0)), start))
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionIdB, events.get(1)),
                                        start.plus(Duration.millis(500))))
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionIdA, events.get(2)),
                                        start.plus(Duration.standardSeconds(1))))
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionIdB, events.get(3)),
                                        start.plus(Duration.standardSeconds(2))))
                        .advanceWatermarkToInfinity();

        PCollectionTuple results =
                pipeline.apply(testStream)
                        .apply(MergeFn.of(stateBaseDir, 2, new DefaultStateStoreProvider()));

        PCollection<Order> successfulOrders = results.get(MergeFn.SUCCESS_TAG);

        PAssert.that(successfulOrders)
                .satisfies(
                        orders -> {
                            int countA = 0;
                            int countB = 0;
                            for (Order order : orders) {
                                if (order.getSessionId().equals(sessionIdA)) countA++;
                                if (order.getSessionId().equals(sessionIdB)) countB++;
                            }
                            Assert.assertEquals(
                                    "Order A should have 2 fragments processed", 2, countA);
                            Assert.assertEquals(
                                    "Order B should have 2 fragments processed", 2, countB);
                            return null;
                        });

        pipeline.run().waitUntilFinish();
    }

    /**
     * Specifically demonstrates the "Warm Up" capability: recovering state from cold storage when a
     * new fragment arrives after the internal state has been cleared.
     */
    @Test
    public void testStateWarmUpFromColdCache() throws Exception {
        String sessionId = "warmup-session";
        Instant start = Instant.ofEpochMilli(0);
        File stateDir = tempFolder.newFolder("state-warmup");
        String stateBaseDir = stateDir.getAbsolutePath();

        List<Event> events = DemoTestUtils.createSingleOrderScenario(sessionId, start);

        TestStream<KV<String, Event>> testStream =
                TestStream.create(
                                KvCoder.of(
                                        StringUtf8Coder.of(),
                                        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault()
                                                .getSchemaCoder(Event.class)))
                        // Send fragment 1 and 2
                        .addElements(TimestampedValue.of(KV.of(sessionId, events.get(0)), start))
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionId, events.get(1)),
                                        start.plus(Duration.standardSeconds(1))))
                        // Advance watermark to trigger offloading (threshold is 2s)
                        .advanceWatermarkTo(start.plus(Duration.standardSeconds(10)))
                        // Send fragment 3 (this should trigger warm up)
                        .addElements(
                                TimestampedValue.of(
                                        KV.of(sessionId, events.get(2)),
                                        start.plus(Duration.standardSeconds(11))))
                        .advanceWatermarkToInfinity();

        PCollectionTuple results =
                pipeline.apply(testStream)
                        .apply(MergeFn.of(stateBaseDir, 2, new DefaultStateStoreProvider()));

        PCollection<Order> successfulOrders = results.get(MergeFn.SUCCESS_TAG);

        PAssert.that(successfulOrders)
                .satisfies(
                        orders -> {
                            Order finalOrder = null;
                            for (Order order : orders) {
                                finalOrder = order;
                            }
                            Assert.assertNotNull("Final order should not be null", finalOrder);
                            Assert.assertEquals(
                                    "Final order should have 3 fragments",
                                    3,
                                    finalOrder.getEvents().size());
                            return null;
                        });

        pipeline.run().waitUntilFinish();
    }
}
