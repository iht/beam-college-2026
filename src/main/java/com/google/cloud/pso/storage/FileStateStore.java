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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of StateStore that uses the local filesystem. Each key is stored in a separate
 * file.
 */
public class FileStateStore implements StateStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileStateStore.class);
    private final String baseDir;

    public FileStateStore(String baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(Paths.get(baseDir));
    }

    private Path getFilePath(String key) {
        // Basic sanitization for key to be a filename
        String sanitizedKey = key.replaceAll("[^a-zA-Z0-9.-]", "_");
        return Paths.get(baseDir, sanitizedKey + ".json");
    }

    @Override
    public String read(String key) throws IOException {
        Path path = getFilePath(key);
        if (!Files.exists(path)) {
            return null;
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                FileLock _ = channel.lock(0, Long.MAX_VALUE, true)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            channel.read(buffer);
            return new String(buffer.array(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Failed to read state for key: " + key, e);
            throw e;
        }
    }

    @Override
    public void update(String key, String data) throws IOException {
        Path path = getFilePath(key);
        try (FileChannel channel =
                        FileChannel.open(
                                path,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.TRUNCATE_EXISTING);
                FileLock _ = channel.lock()) {
            ByteBuffer buffer = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
            channel.write(buffer);
        } catch (IOException e) {
            LOG.error("Failed to update state for key: " + key, e);
            throw e;
        }
    }

    @Override
    public void clear(String key) throws IOException {
        Path path = getFilePath(key);
        // Use a lock even for deletion if possible, but Files.delete doesn't support locking
        // directly.
        // However, since update and read use exclusive/shared locks, we can try to acquire an
        // exclusive lock
        // before deleting to ensure no one else is using it.
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
                FileLock _ = channel.lock()) {
            // Just acquiring the lock is enough to know it's safe to delete.
        } catch (java.nio.file.NoSuchFileException e) {
            return; // Already deleted
        }

        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws Exception {
        // No specific resources to close at the store level
    }
}
