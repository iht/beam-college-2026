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
import java.io.File;
import java.util.List;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class DemoTestUtils {

    public static Event createEvent(
            String sessionId,
            String eventType,
            int sequence,
            int totalFragments,
            Instant timestamp) {
        Event event = new Event();
        event.setSessionId(sessionId);
        event.setEventType(eventType);
        event.setSequence(sequence);
        event.setTotalFragments(totalFragments);
        event.setTimestamp(timestamp.getMillis());
        return event;
    }

    public static List<Event> createSingleOrderScenario(String sessionId, Instant start) {
        return List.of(
                createEvent(sessionId, "fragment", 1, 3, start),
                createEvent(sessionId, "fragment", 2, 3, start.plus(Duration.standardSeconds(1))),
                createEvent(sessionId, "fragment", 3, 3, start.plus(Duration.standardSeconds(5))));
    }

    public static List<Event> createInterleavedOrdersScenario(
            String sessionIdA, String sessionIdB, Instant start) {
        return List.of(
                createEvent(sessionIdA, "fragment", 1, 2, start),
                createEvent(sessionIdB, "fragment", 1, 2, start.plus(Duration.millis(500))),
                createEvent(sessionIdA, "fragment", 2, 2, start.plus(Duration.standardSeconds(1))),
                createEvent(sessionIdB, "fragment", 2, 2, start.plus(Duration.standardSeconds(2))));
    }

    public static List<Event> createIncompleteOrderScenario(String sessionId, Instant start) {
        return List.of(
                createEvent(sessionId, "fragment", 1, 3, start),
                createEvent(sessionId, "fragment", 2, 3, start.plus(Duration.standardSeconds(1))));
    }

    public static boolean checkStateFileExists(String baseDir, String sessionId) {
        File file = new File(baseDir, sessionId + ".json");
        return file.exists();
    }
}
