/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageManagerTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.TableBasedMetadataStore;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for {@link TableBasedMetadataStore} with {@link InMemoryChunkStorageProvider} using {@link SimpleStorageTests}.
 */
public class TableBasedMetadataSimpleStorageTests extends SimpleStorageTests {

    protected ChunkStorageProvider getChunkStorage()  throws Exception {
        return new InMemoryChunkStorageProvider();
    }

    protected ChunkMetadataStore getMetadataStore()  throws Exception {
        TableStore tableStore = new InMemoryTableStore(executorService());
        String tableName = "TableBasedMetadataSimpleStorageTests";
        return  new TableBasedMetadataStore(tableName, tableStore);
    }

    @Test
    @Override
    public void testZombieFencing() throws Exception {
        //TableBasedMetadataStore does not support clone.
    }

    /**
     * Unit tests for {@link TableBasedMetadataStore} using {@link ChunkManagerRollingTests}.
     */
    public static class InMemorySimpleStorageRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage()  throws Exception {
            return new InMemoryChunkStorageProvider();
        }

        protected ChunkMetadataStore getMetadataStore()  throws Exception {
            TableStore tableStore = new InMemoryTableStore(executorService());
            String tableName = "TableBasedMetadataSimpleStorageTests";
            return  new TableBasedMetadataStore(tableName, tableStore);
        }
    }

    /**
     * Unit tests for {@link TableBasedMetadataStore} using {@link ChunkStorageManagerTests}.
     */
    public static class InMemorySimpleStorage extends ChunkStorageManagerTests {
        @Override
        public ChunkMetadataStore createMetadataStore() throws Exception {
            TableStore tableStore = new InMemoryTableStore(Executors.newScheduledThreadPool(1));
            String tableName = "TableBasedMetadataSimpleStorageTests";
            return new TableBasedMetadataStore(tableName, tableStore);
        }

        public TestContext getTestContext()  throws Exception {
            return new InMemorySimpleStorageTestContext(executorService());
        }

        public static class InMemorySimpleStorageTestContext extends TestContext {
            InMemorySimpleStorageTestContext(ExecutorService executorService)  throws Exception {
                super(executorService);
            }

            @Override
            public ChunkMetadataStore createMetadataStore() throws Exception {
                return createChunkMetadataStore();
            }

            private ChunkMetadataStore createChunkMetadataStore() {
                TableStore tableStore = new InMemoryTableStore(executor);
                String tableName = "TableBasedMetadataSimpleStorageTests";
                return new TableBasedMetadataStore(tableName, tableStore);
            }
        }
    }
}
