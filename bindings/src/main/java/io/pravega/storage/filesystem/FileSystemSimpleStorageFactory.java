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

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;

import java.util.concurrent.ExecutorService;

/**
 * Factory for ExtendedS3 {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link FileSystemChunkStorage}.
 */
public class FileSystemSimpleStorageFactory implements StorageFactory {
    private final FileSystemStorageConfig config;
    private final ExecutorService executor;

    /**
     * Creates a new instance of the {@link FileSystemSimpleStorageFactory} class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public FileSystemSimpleStorageFactory(FileSystemStorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        ChunkedSegmentStorage storageProvider = new ChunkedSegmentStorage(
                new FileSystemChunkStorage(this.config),
                this.executor,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        return storageProvider;
    }
}