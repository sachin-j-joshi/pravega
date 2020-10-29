/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;

import java.util.concurrent.Executor;

/**
 * Defines a Factory for Simple Storage Adapters.
 */
public interface SimpleStorageFactory extends StorageFactory {
    /**
     * Creates a new instance of a Storage adapter.
     *
     * @param metadataStore {@link ChunkMetadataStore} store to use.
     */
    Storage createStorageAdapter(ChunkMetadataStore metadataStore);

    /**
     * Gets the executor used by the factory.
     * @return Executor used by the factory.
     */
    Executor getExecutor();
}
