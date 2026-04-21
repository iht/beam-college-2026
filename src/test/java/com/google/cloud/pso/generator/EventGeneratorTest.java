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

package com.google.cloud.pso.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.pso.model.Event;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Tests for EventGenerator. */
public class EventGeneratorTest {

    @Test
    public void testGenerateSessionEvents() {
        String sessionId = "test-session";
        long startTimestamp = 1000L;
        List<Event> events = EventGenerator.generateSessionEvents(sessionId, startTimestamp);

        // Verify total event count
        assertEquals(30, events.size());

        // Verify session ID
        for (Event event : events) {
            assertEquals(sessionId, event.getSessionId());
        }

        // Verify terminal events
        assertEquals("ADD_PAYMENT", events.get(28).getEventType());
        assertEquals("SUBMIT_ORDER", events.get(29).getEventType());
    }

    @Test
    public void testGenerateMultipleSessions() {
        int numSessions = 5;
        long startTimestamp = System.currentTimeMillis();
        List<Event> allEvents =
                EventGenerator.generateMultipleSessions(numSessions, startTimestamp);

        // Verify total count (30 events per session)
        assertEquals(numSessions * 30, allEvents.size());

        // Verify unique session IDs
        Set<String> sessionIds =
                allEvents.stream().map(Event::getSessionId).collect(Collectors.toSet());
        assertEquals(numSessions, sessionIds.size());
    }
}
