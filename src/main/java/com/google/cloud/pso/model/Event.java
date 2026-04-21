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
import java.util.Map;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** POJO representing a shopping event. */
@DefaultCoder(SerializableCoder.class)
public class Event implements Serializable {

    @JsonProperty("session_id")
    private String sessionId;

    private Long timestamp;

    @JsonProperty("event_type")
    private String eventType;

    private Map<String, Object> data;

    @JsonProperty("sequence")
    private Integer sequence;

    @JsonProperty("total_fragments")
    private Integer totalFragments;

    public Event() {}

    public Event(String sessionId, Long timestamp, String eventType, Map<String, Object> data) {
        this.sessionId = sessionId;
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.data = data;
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

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Integer getSequence() {
        return sequence;
    }

    public void setSequence(Integer sequence) {
        this.sequence = sequence;
    }

    public Integer getTotalFragments() {
        return totalFragments;
    }

    public void setTotalFragments(Integer totalFragments) {
        this.totalFragments = totalFragments;
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
