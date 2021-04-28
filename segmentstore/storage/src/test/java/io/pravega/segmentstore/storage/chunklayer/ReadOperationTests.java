/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * This class tests {@link ReadOperation}.
 */
public class ReadOperationTests extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(3000);
    private static final int CONTAINER_ID = 42;
    private static final int OWNER_EPOCH = 100;
    protected final Random rnd = new Random(0);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    protected int getThreadPoolSize() {
        return 5;
    }

    @Test
    public void testParallelReadSingleChunk() throws Exception {
        testParallelReadSingleChunk(40, 9, 50, 10);
        for (int i = 0; i < 5; i++) {
            int start = 10 * i;
            testParallelReadSingleChunk(10 * i, 50 - start, 50, 10);
        }
        testParallelReadSingleChunk(3, 42, 50, 10);
        testParallelReadSingleChunk(1, 19, 20, 10);
        testParallelReadSingleChunk(9, 11, 20, 10);
        testParallelReadSingleChunk(0, 42, 50, 10);
    }

    private void testParallelReadSingleChunk(int from, int length, int size, int readBlockSize) throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .readBlockSize(readBlockSize)
                .build();
        @Cleanup
        BaseMetadataStore metadataStore = new InMemoryMetadataStore(config, executorService());
        @Cleanup
        TestChunkStorage chunkStorage = new TestChunkStorage(executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, chunkStorage, metadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);

        val h = chunkedSegmentStorage.create("test", new SegmentRollingPolicy(2L * size), null).join();
        val writeBuffer = new byte[size];
        rnd.nextBytes(writeBuffer);

        chunkedSegmentStorage.write(h, 0, new ByteArrayInputStream(writeBuffer), size, null).join();

        val expected = Arrays.copyOfRange(writeBuffer, from, from + length);
        val readBuffer = new byte[length];

        chunkedSegmentStorage.read(h, from, readBuffer, 0, readBuffer.length, null).join();

        Assert.assertArrayEquals(expected, readBuffer);

        val chunks = TestUtils.getChunkList(metadataStore, "test");

        int start = Math.toIntExact((long) config.getReadBlockSize() * (from / config.getReadBlockSize()));
        int readCount = 0;
        int bytesCount = 0;
        int offsetInChunk = from;
        int remaining = length;
        if (start < from) {
            int toRead = config.getReadBlockSize() - offsetInChunk;
            assertIsCalled(chunkStorage, ReadArgs.builder()
                    .bufferOffset(0)
                    .fromOffset(offsetInChunk)
                    .chunkName(chunks.get(0).getName())
                    .length(toRead)
                    .build());
            readCount++;
            bytesCount += toRead;
            offsetInChunk += toRead;
            remaining -= toRead;
        }
        while (remaining > 0) {
            int toRead = Math.min(remaining, config.getReadBlockSize());
            assertIsCalled(chunkStorage, ReadArgs.builder()
                    .bufferOffset(bytesCount)
                    .fromOffset(offsetInChunk)
                    .chunkName(chunks.get(0).getName())
                    .length(toRead)
                    .build());
            readCount++;
            bytesCount += toRead;
            offsetInChunk += toRead;
            remaining -= toRead;
        }
        Assert.assertEquals(readCount, chunkStorage.getInvocations().size());
    }

    private void assertIsCalled(TestChunkStorage chunkStorage, ReadArgs arg) {
        for (val v :chunkStorage.getInvocations()) {
            if (v.equals(arg)) {
                return;
            }
        }
        Assert.fail(String.format("Not Called :%s", arg));
    }

    @Test
    public void testParallelReadMultipleChunks() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .readBlockSize(10)
                .build();

        @Cleanup
        BaseMetadataStore metadataStore = new InMemoryMetadataStore(config, executorService());
        @Cleanup
        TestChunkStorage chunkStorage = new TestChunkStorage(executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, chunkStorage, metadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);

        val h = chunkedSegmentStorage.create("test", new SegmentRollingPolicy(25), null).join();
        val writeBuffer = new byte[30];
        rnd.nextBytes(writeBuffer);

        chunkedSegmentStorage.write(h, 0, new ByteArrayInputStream(writeBuffer), 30, null).join();

        val expected = Arrays.copyOfRange(writeBuffer, 2,  27);
        val readBuffer = new byte[25];

        chunkedSegmentStorage.read(h, 2, readBuffer, 0, 25, null).join();

        Assert.assertArrayEquals(expected, readBuffer);

        val chunks = TestUtils.getChunkList(metadataStore, "test");

        Assert.assertEquals(4, chunkStorage.getInvocations().size());
        assertIsCalled(chunkStorage, ReadArgs.builder()
                .bufferOffset(0)
                .fromOffset(2)
                .chunkName(chunks.get(0).getName())
                .length(8)
                .build());
        assertIsCalled(chunkStorage, ReadArgs.builder()
                .bufferOffset(8)
                .fromOffset(10)
                .chunkName(chunks.get(0).getName())
                .length(10)
                .build());
        assertIsCalled(chunkStorage, ReadArgs.builder()
                .bufferOffset(18)
                .fromOffset(20)
                .chunkName(chunks.get(0).getName())
                .length(5)
                .build());
        assertIsCalled(chunkStorage, ReadArgs.builder()
                .bufferOffset(23)
                .fromOffset(0)
                .chunkName(chunks.get(1).getName())
                .length(2)
                .build());
    }

    static class TestChunkStorage extends InMemoryChunkStorage {
        @Getter
        Set<ReadArgs> invocations = Collections.synchronizedSet(new HashSet<ReadArgs>());
        TestChunkStorage(Executor executor) {
            super(executor);
        }

        @Override
        protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
            invocations.add(ReadArgs.builder()
                    //.buffer(buffer)
                    .bufferOffset(bufferOffset)
                    .fromOffset(fromOffset)
                    .chunkName(handle.getChunkName())
                    .length(length)
                    .build());
            return super.doRead(handle, fromOffset, length, buffer, bufferOffset);
        }
    }

    @Data
    @Builder
    static class ReadArgs {
        private String chunkName;
        private long fromOffset;
        private int length;
        private int bufferOffset;
    }
}
