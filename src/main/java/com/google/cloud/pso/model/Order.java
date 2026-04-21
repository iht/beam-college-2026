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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Represents the aggregated state of an order based on shopping events. */
@DefaultCoder(SerializableCoder.class)
public class Order implements Serializable {
    @JsonProperty("session_id")
    private String sessionId;

    private List<Event> events = new ArrayList<>();
    private Map<String, Integer> items = new HashMap<>();

    @JsonProperty("payment_method")
    private String paymentMethod;

    private String status = "NEW";

    public Order() {}

    public Order(String sessionId) {
        this.sessionId = sessionId;
    }

    public void addEvent(Event event) {
        if (this.sessionId == null) {
            this.sessionId = event.getSessionId();
        }
        events.add(event);
        recalculate();
    }

    public void recalculate() {
        // Reset state
        items = new HashMap<>();
        paymentMethod = null;
        status = "NEW";

        // Sort events by timestamp to handle out-of-order arrival
        events.sort(Comparator.comparing(Event::getTimestamp));

        // Re-apply all events
        for (Event event : events) {
            apply(event);
        }
    }

    private void apply(Event event) {
        String eventType = event.getEventType();
        Map<String, Object> data = event.getData();

        switch (eventType) {
            case "ADD_TO_CART":
                String addItemId = (String) data.get("item_id");
                Object qtyObj = data.get("quantity");
                Integer quantity = 0;
                if (qtyObj instanceof Integer) {
                    quantity = (Integer) qtyObj;
                } else if (qtyObj instanceof Long) {
                    quantity = ((Long) qtyObj).intValue();
                } else if (qtyObj instanceof String) {
                    quantity = Integer.parseInt((String) qtyObj);
                }
                items.put(addItemId, items.getOrDefault(addItemId, 0) + quantity);
                break;
            case "REMOVE_FROM_CART":
                String removeItemId = (String) data.get("item_id");
                items.remove(removeItemId);
                break;
            case "ADD_PAYMENT":
                paymentMethod = (String) data.get("payment_method");
                status = "PAYING";
                break;
            case "SUBMIT_ORDER":
                status = "SUBMITTED";
                break;
        }
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public Map<String, Integer> getItems() {
        return items;
    }

    public void setItems(Map<String, Integer> items) {
        this.items = items;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Order{"
                + "sessionId='"
                + sessionId
                + '\''
                + ", events="
                + events
                + ", items="
                + items
                + ", paymentMethod='"
                + paymentMethod
                + '\''
                + ", status='"
                + status
                + '\''
                + '}';
    }
}
