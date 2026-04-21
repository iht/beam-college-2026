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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OrderRobustnessTest {

    @Test
    public void testMultipleAdditionsAndRemovals() {
        Order order = new Order("session-robust");

        // 1. Add item p1, quantity 2
        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 2);
        order.addEvent(new Event("session-robust", 100L, "ADD_TO_CART", data1));

        // 2. Add item p1, quantity 3
        Map<String, Object> data2 = new HashMap<>();
        data2.put("item_id", "p1");
        data2.put("quantity", 3);
        order.addEvent(new Event("session-robust", 200L, "ADD_TO_CART", data2));

        assertEquals(5, order.getItems().get("p1"));

        // 3. Remove item p1
        Map<String, Object> data3 = new HashMap<>();
        data3.put("item_id", "p1");
        order.addEvent(new Event("session-robust", 300L, "REMOVE_FROM_CART", data3));

        assertFalse(order.getItems().containsKey("p1"));

        // 4. Add item p1 again, quantity 1
        Map<String, Object> data4 = new HashMap<>();
        data4.put("item_id", "p1");
        data4.put("quantity", 1);
        order.addEvent(new Event("session-robust", 400L, "ADD_TO_CART", data4));

        assertEquals(1, order.getItems().get("p1"));
    }

    @Test
    public void testOutOfOrderEvents() {
        Order order = new Order("session-robust");

        // Events out of order: T3, T1, T2
        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 2);
        Event e1 = new Event("session-robust", 100L, "ADD_TO_CART", data1);

        Map<String, Object> data2 = new HashMap<>();
        data2.put("item_id", "p1");
        data2.put("quantity", 3);
        Event e2 = new Event("session-robust", 200L, "ADD_TO_CART", data2);

        Map<String, Object> data3 = new HashMap<>();
        data3.put("item_id", "p1");
        Event e3 = new Event("session-robust", 150L, "REMOVE_FROM_CART", data3);

        order.addEvent(e3); // T3 first
        order.addEvent(e1); // T1 second
        order.addEvent(e2); // T2 last

        // Final order: T1(ADD), T3(REMOVE), T2(ADD)
        // State:
        // T1: {p1: 2}
        // T3: {}
        // T2: {p1: 3}
        assertEquals(3, order.getItems().get("p1"));
    }

    @Test
    public void testSubmitOrderBeforePayment() {
        Order order = new Order("session-robust");

        order.addEvent(new Event("session-robust", 200L, "SUBMIT_ORDER", new HashMap<>()));
        assertEquals("SUBMITTED", order.getStatus());

        Map<String, Object> payData = new HashMap<>();
        payData.put("payment_method", "paypal");
        order.addEvent(new Event("session-robust", 100L, "ADD_PAYMENT", payData));

        // After recalculation: T1(ADD_PAYMENT), T2(SUBMIT_ORDER)
        // State:
        // T1: status=PAYING, paymentMethod=paypal
        // T2: status=SUBMITTED
        assertEquals("SUBMITTED", order.getStatus());
        assertEquals("paypal", order.getPaymentMethod());
    }

    @Test
    public void testRemoveNonExistentItem() {
        Order order = new Order("session-robust");

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        order.addEvent(new Event("session-robust", 100L, "REMOVE_FROM_CART", data1));

        assertTrue(order.getItems().isEmpty());
    }

    @Test
    public void testVariousQuantityTypes() {
        Order order = new Order("session-robust");

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 2); // Integer
        order.addEvent(new Event("session-robust", 100L, "ADD_TO_CART", data1));

        Map<String, Object> data2 = new HashMap<>();
        data2.put("item_id", "p1");
        data2.put("quantity", 3L); // Long
        order.addEvent(new Event("session-robust", 200L, "ADD_TO_CART", data2));

        Map<String, Object> data3 = new HashMap<>();
        data3.put("item_id", "p1");
        data3.put("quantity", "5"); // String
        order.addEvent(new Event("session-robust", 300L, "ADD_TO_CART", data3));

        assertEquals(10, order.getItems().get("p1"));
    }

    @Test
    public void testDoubleQuantityType() {
        Order order = new Order("session-robust");

        Map<String, Object> data1 = new HashMap<>();
        data1.put("item_id", "p1");
        data1.put("quantity", 2.5); // Double
        order.addEvent(new Event("session-robust", 100L, "ADD_TO_CART", data1));

        // Current implementation will result in 0
        assertEquals(0, order.getItems().get("p1"));
    }
}
