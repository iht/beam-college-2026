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

package com.google.cloud.pso.storage;

import java.io.IOException;
import java.io.Serializable;

/** Interface for a key-value state store used by the SessionMergePipeline. */
public interface StateStore extends Serializable, AutoCloseable {

    /**
     * Reads the state data associated with the given key.
     *
     * @param key The key to read.
     * @return The state data as a String, or null if not found.
     * @throws IOException If an error occurs during reading.
     */
    String read(String key) throws IOException;

    /**
     * Updates (or creates) the state data associated with the given key.
     *
     * @param key The key to update.
     * @param data The data to store.
     * @throws IOException If an error occurs during writing.
     */
    void update(String key, String data) throws IOException;

    /**
     * Clears (deletes) the state data associated with the given key.
     *
     * @param key The key to clear.
     * @throws IOException If an error occurs during deletion.
     */
    void clear(String key) throws IOException;
}
