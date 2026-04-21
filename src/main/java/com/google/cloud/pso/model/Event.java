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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Java Bean representing a shopping event. */
@DefaultSchema(JavaBeanSchema.class)
public class Event implements Comparable<Event> {

    @JsonProperty("session_id")
    private String sessionId;

    private Long timestamp;

    @JsonProperty("event_type")
    private String eventType;

    private Map<String, String> data;

    @JsonProperty("sequence")
    @Nullable private Integer sequence;

    @JsonProperty("total_fragments")
    @Nullable private Integer totalFragments;

    public Event() {
        this.data = new java.util.HashMap<>();
    }

    public Event(String sessionId, Long timestamp, String eventType, Map<String, ?> data) {
        this.sessionId = sessionId;
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.data = new HashMap<>();
        if (data != null) {
            for (Map.Entry<String, ?> entry : data.entrySet()) {
                this.data.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    @Nullable public Integer getSequence() {
        return sequence;
    }

    public void setSequence(@Nullable Integer sequence) {
        this.sequence = sequence;
    }

    @Nullable public Integer getTotalFragments() {
        return totalFragments;
    }

    public void setTotalFragments(@Nullable Integer totalFragments) {
        this.totalFragments = totalFragments;
    }

    @Override
    public int compareTo(Event other) {
        return this.timestamp.compareTo(other.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return Objects.equals(sessionId, event.sessionId)
                && Objects.equals(timestamp, event.timestamp)
                && Objects.equals(eventType, event.eventType)
                && Objects.equals(data, event.data)
                && Objects.equals(sequence, event.sequence)
                && Objects.equals(totalFragments, event.totalFragments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, timestamp, eventType, data, sequence, totalFragments);
    }

    @Override
    public String toString() {
        return "Event{"
                + "sessionId='"
                + sessionId
                + '\''
                + ", timestamp="
                + timestamp
                + ", eventType='"
                + eventType
                + '\''
                + ", data="
                + data
                + ", sequence="
                + sequence
                + ", totalFragments="
                + totalFragments
                + '}';
    }
}
