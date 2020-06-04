/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class FileSystemSimpleStorageTest extends SimpleStorageTests {
    private static ChunkStorageProvider getChunkStorageProvider() throws IOException {
        File baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        return new FileSystemChunkStorageProvider(FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, baseDir.getAbsolutePath())
                .build());
    }

    protected ChunkStorageProvider getChunkStorage()  throws Exception {
        return getChunkStorageProvider();
    }

    /**
     * {@link ChunkManagerRollingTests} tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage()  throws Exception {
            return getChunkStorageProvider();
        }

    }

    /**
     * {@link ChunkStorageProviderTests} tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageProviderTests extends ChunkStorageProviderTests {
        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return getChunkStorageProvider();
        }

        /**
         * Test default capabilities.
         */
        @Test
        public void testCapabilities() {
            assertEquals(true, getChunkStorage().supportsAppend());
            assertEquals(false, getChunkStorage().supportsTruncation());
            assertEquals(true, getChunkStorage().supportsConcat());
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageSystemJournalTests extends SystemJournalTests {
        @Override
        protected ChunkStorageProvider getChunkStorageProvider() throws Exception {
            return FileSystemSimpleStorageTest.getChunkStorageProvider();
        }
    }
}
