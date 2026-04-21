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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OrderTest {

    @Test
    public void testEventApplicationOrder() {
        Order order = new Order("session-1");

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 1);
        Event e1 = new Event("session-1", 100L, "ADD_TO_CART", data1);

        Map<String, Object> data2 = new HashMap<>();
        data2.put("item_id", "p2");
        data2.put("quantity", 2);
        Event e2 = new Event("session-1", 200L, "ADD_TO_CART", data2);

        // Add out of order
        order.addEvent(e2);
        order.addEvent(e1);

        assertEquals(2, order.getItems().size());
        assertEquals(Integer.valueOf(1), order.getItems().get("p1"));
        assertEquals(Integer.valueOf(2), order.getItems().get("p2"));
        assertEquals(2, order.getEvents().size());
    }

    @Test
    public void testStatusTransitions() {
        Order order = new Order("session-1");
        assertEquals("NEW", order.getStatus());

        Map<String, Object> payData = new HashMap<>();
        payData.put("payment_method", "credit_card");
        order.addEvent(new Event("session-1", 100L, "ADD_PAYMENT", payData));
        assertEquals("PAYING", order.getStatus());
        assertEquals("credit_card", order.getPaymentMethod());

        order.addEvent(new Event("session-1", 200L, "SUBMIT_ORDER", new HashMap<>()));
        assertEquals("SUBMITTED", order.getStatus());
    }

    @Test
    public void testItemQuantityAggregation() {
        Order order = new Order("session-1");

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 2);
        order.addEvent(new Event("session-1", 100L, "ADD_TO_CART", data1));

        Map<String, Object> data2 = new HashMap<>();
        data2.put("item_id", "p1");
        data2.put("quantity", 3);
        order.addEvent(new Event("session-1", 200L, "ADD_TO_CART", data2));

        assertEquals(Integer.valueOf(5), order.getItems().get("p1"));

        Map<String, Object> data3 = new HashMap<>();
        data3.put("item_id", "p1");
        order.addEvent(new Event("session-1", 300L, "REMOVE_FROM_CART", data3));

        assertEquals(0, order.getItems().size());
    }
}
