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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileStateStoreTest {

    @TempDir Path tempDir;
    private FileStateStore stateStore;

    @BeforeEach
    public void setUp() throws IOException {
        stateStore = new FileStateStore(tempDir.toString());
    }

    @Test
    public void testReadWriteClear() throws IOException {
        String key = "session-1";
        String data = "{\"sessionId\":\"session-1\",\"items\":[]}";

        // Initial read should be null
        assertNull(stateStore.read(key));

        // Update state
        stateStore.update(key, data);

        // Read back state
        assertEquals(data, stateStore.read(key));

        // Clear state
        stateStore.clear(key);

        // Read should be null again
        assertNull(stateStore.read(key));
    }

    @Test
    public void testMultipleKeys() throws IOException {
        stateStore.update("key1", "data1");
        stateStore.update("key2", "data2");

        assertEquals("data1", stateStore.read("key1"));
        assertEquals("data2", stateStore.read("key2"));
    }
}
