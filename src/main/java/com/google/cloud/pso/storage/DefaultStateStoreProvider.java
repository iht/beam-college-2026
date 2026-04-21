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
import java.io.UncheckedIOException;

/** Default implementation of StateStoreProvider that provides a FileStateStore. */
public class DefaultStateStoreProvider implements StateStoreProvider {

    @Override
    public StateStore getStateStore(String baseDir) {
        try {
            return new FileStateStore(baseDir);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to create FileStateStore in directory: " + baseDir, e);
        }
    }
}
