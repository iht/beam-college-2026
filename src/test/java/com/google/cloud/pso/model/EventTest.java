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

package com.google.cloud.pso.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class EventTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testSerializationDeserialization() throws IOException {
        Map<String, Object> data = new HashMap<>();
        data.put("item_id", "abc");

        Event event = new Event("session-1", 12345L, "ADD_TO_CART", data);
        event.setSequence(1);
        event.setTotalFragments(2);

        String json = OBJECT_MAPPER.writeValueAsString(event);
        Event deserialized = OBJECT_MAPPER.readValue(json, Event.class);

        assertEquals(event.getSessionId(), deserialized.getSessionId());
        assertEquals(event.getTimestamp(), deserialized.getTimestamp());
        assertEquals(event.getEventType(), deserialized.getEventType());
        assertEquals(event.getSequence(), deserialized.getSequence());
        assertEquals(event.getTotalFragments(), deserialized.getTotalFragments());
    }
}
