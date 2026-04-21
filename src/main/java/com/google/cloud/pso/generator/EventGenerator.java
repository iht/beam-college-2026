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

import com.google.cloud.pso.model.Event;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/** Logic for generating, shuffling, and serializing shopping events. */
public class EventGenerator {

    private static final Random RANDOM = new Random();

    static List<Event> generateSessionEvents(String sessionId, long baseTimestamp) {
        List<Event> events = new ArrayList<>();
        long currentTimestamp = baseTimestamp;
        List<String> cart = new ArrayList<>();
        int itemCounter = 1;

        // Generate 28 shopping events (ADD_TO_CART or REMOVE_FROM_CART)
        for (int i = 0; i < 28; i++) {
            Event event;
            // If cart is empty, we MUST add something
            // Otherwise, 70% chance to add, 30% to remove
            if (cart.isEmpty() || RANDOM.nextDouble() < 0.7) {
                String itemId = "p_" + itemCounter++;
                cart.add(itemId);
                Map<String, String> data = new HashMap<>();
                data.put("item_id", itemId);
                data.put("quantity", String.valueOf(RANDOM.nextInt(5) + 1));
                event = new Event(sessionId, currentTimestamp, "ADD_TO_CART", data);
            } else {
                String itemToRemove = cart.get(RANDOM.nextInt(cart.size()));
                cart.remove(itemToRemove);
                Map<String, String> data = new HashMap<>();
                data.put("item_id", itemToRemove);
                event = new Event(sessionId, currentTimestamp, "REMOVE_FROM_CART", data);
            }
            event.setSequence(i + 1);
            event.setTotalFragments(30);
            events.add(event);

            currentTimestamp += (RANDOM.nextInt(4001) + 1000); // 1000 to 5000ms
        }

        // Ensure at least one item remains in the cart for payment
        if (cart.isEmpty()) {
            String itemId = "p_" + itemCounter++;
            Map<String, String> data = new HashMap<>();
            data.put("item_id", itemId);
            data.put("quantity", "1");
            // Replace the last event to maintain exactly 30 total events
            Event lastEvent = events.get(events.size() - 1);
            Event newEvent = new Event(sessionId, lastEvent.getTimestamp(), "ADD_TO_CART", data);
            newEvent.setSequence(lastEvent.getSequence());
            newEvent.setTotalFragments(lastEvent.getTotalFragments());
            events.set(events.size() - 1, newEvent);
            cart.add(itemId);
        }

        // Add payment
        Map<String, String> paymentData = new HashMap<>();
        String method = (new String[] {"credit_card", "paypal", "apple_pay"})[RANDOM.nextInt(3)];
        paymentData.put("payment_method", method);
        Event paymentEvent = new Event(sessionId, currentTimestamp, "ADD_PAYMENT", paymentData);
        paymentEvent.setSequence(29);
        paymentEvent.setTotalFragments(30);
        events.add(paymentEvent);
        currentTimestamp += (RANDOM.nextInt(4001) + 1000);

        // Submit order
        Event submitEvent = new Event(sessionId, currentTimestamp, "SUBMIT_ORDER", new HashMap<>());
        submitEvent.setSequence(30);
        submitEvent.setTotalFragments(30);
        events.add(submitEvent);

        return events;
    }

    public static List<Event> generateMultipleSessions(int numSessions, long startTimestamp) {
        List<Event> allEvents = new ArrayList<>();
        for (int i = 0; i < numSessions; i++) {
            String sessionId = UUID.randomUUID().toString();
            long sessionStart = startTimestamp + RANDOM.nextInt(30001);
            allEvents.addAll(generateSessionEvents(sessionId, sessionStart));
        }
        // Shuffle all events to randomize publication order
        Collections.shuffle(allEvents);
        return allEvents;
    }
}
