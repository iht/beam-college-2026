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

package com.google.cloud.pso.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface SessionMergeOptions extends org.apache.beam.sdk.extensions.gcp.options.GcpOptions {
    @Description("Base directory for local file state storage")
    @Required
    String getStateBaseDir();

    void setStateBaseDir(String value);

    @Description("Threshold in seconds to move state to external storage")
    @Default.Integer(2)
    Integer getStateMoveThresholdSeconds();

    void setStateMoveThresholdSeconds(Integer value);

    @Description("Number of events to generate internally")
    @Default.Integer(100)
    Integer getNumEvents();

    void setNumEvents(Integer value);
}
