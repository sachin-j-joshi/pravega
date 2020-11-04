/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_INDEX_SCAN_LATENCY;

@Slf4j
class ReadOperation implements Callable<CompletableFuture<Integer>> {
    private final SegmentHandle handle;
    private final long offset;
    private final byte[] buffer;
    private final int bufferOffset;
    private final int length;
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final long traceId;
    private final Timer timer;
    private final AtomicInteger cntScanned = new AtomicInteger();

    static class State {
        private SegmentMetadata segmentMetadata;
        private int bytesRemaining;
        private int currentBufferOffset;
        private long currentOffset;
        private int totalBytesRead = 0;
        private long startOffsetForCurrentChunk;
        private String currentChunkName;
        private ChunkMetadata chunkToReadFrom = null;
        private boolean isLoopExited;
        private int bytesToRead;
    }

    ReadOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) {
        this.handle = handle;
        this.offset = offset;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
        this.length = length;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
        timer = new Timer();
    }

    public CompletableFuture<Integer> call() {
        val state = new State();
        // Validate preconditions.
        checkPreconditions(state);
        log.debug("{} read - started op={}, segment={}, offset={}, bytesRead={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, state.totalBytesRead);
        val streamSegmentName = handle.getSegmentName();
        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(streamSegmentName),
                txn -> txn.get(streamSegmentName)
                        .thenApplyAsync(storageMetadata -> {
                            state.segmentMetadata = (SegmentMetadata) storageMetadata;
                            return state;
                        }).thenComposeAsync(state0 -> {
                            // Validate preconditions.
                            checkState(state0);

                            if (length == 0) {
                                return CompletableFuture.completedFuture(0);
                            }

                            return findChunkForOffset(state0, txn)
                                    .thenComposeAsync(state1 -> {
                                        // Now read.
                                        return readData(state1, txn);
                                    }, chunkedSegmentStorage.getExecutor())
                                    .exceptionally(ex -> {
                                        //log.debug("{} read - started op={}, segment={}, offset={}, bytesRead={}.",
                                        //        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, state1.totalBytesRead);
                                        if (ex instanceof CompletionException) {
                                            throw (CompletionException) ex;
                                        }
                                        throw new CompletionException(ex);
                                    })
                                    .thenApplyAsync(state2 -> {
                                        logEnd(state2);
                                        return state2.totalBytesRead;
                                    }, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor());
    }

    private void logEnd(State state) {
        Duration elapsed = timer.getElapsed();
        SLTS_READ_LATENCY.reportSuccessEvent(elapsed);
        SLTS_READ_BYTES.add(length);
        if (chunkedSegmentStorage.getConfig().getLateWarningThresholdInMillis() < elapsed.toMillis()) {
            log.warn("{} read - late op={}, segment={}, offset={}, bytesRead={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, state.totalBytesRead, elapsed.toMillis());
        } else {
            log.debug("{} read - finished op={}, segment={}, offset={}, bytesRead={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, state.totalBytesRead, elapsed.toMillis());
        }
        LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, state.totalBytesRead);
    }

    private CompletableFuture<State> readData(State state, MetadataTransaction txn) {
        return Futures.loop(
                () -> state.bytesRemaining > 0 && null != state.currentChunkName,
                () -> {
                    state.bytesToRead = Math.min(state.bytesRemaining, Math.toIntExact(state.chunkToReadFrom.getLength() - (state.currentOffset - state.startOffsetForCurrentChunk)));
                    if (state.currentOffset >= state.startOffsetForCurrentChunk + state.chunkToReadFrom.getLength()) {
                        // The current chunk is over. Move to the next one.
                        state.currentChunkName = state.chunkToReadFrom.getNextChunk();
                        if (null != state.currentChunkName) {
                            state.startOffsetForCurrentChunk += state.chunkToReadFrom.getLength();
                            return txn.get(state.currentChunkName)
                                    .thenApplyAsync(storageMetadata -> {
                                        state.chunkToReadFrom = (ChunkMetadata) storageMetadata;
                                        log.debug("{} read - reading from next chunk - segment={}, chunk={}", chunkedSegmentStorage.getLogPrefix(), handle.getSegmentName(), state.chunkToReadFrom);
                                        return null;
                                    }, chunkedSegmentStorage.getExecutor());
                        }
                    } else {
                        Preconditions.checkState(state.bytesToRead != 0, "bytesToRead is 0");
                        // Read data from the chunk.
                        return chunkedSegmentStorage.getChunkStorage().openRead(state.chunkToReadFrom.getName())
                                .thenComposeAsync(chunkHandle ->
                                        chunkedSegmentStorage.getChunkStorage().read(chunkHandle,
                                                state.currentOffset - state.startOffsetForCurrentChunk,
                                                state.bytesToRead,
                                                buffer,
                                                state.currentBufferOffset)
                                                .thenApplyAsync(bytesRead -> {
                                                    state.bytesRemaining -= bytesRead;
                                                    state.currentOffset += bytesRead;
                                                    state.currentBufferOffset += bytesRead;
                                                    state.totalBytesRead += bytesRead;
                                                    return null;
                                                }, chunkedSegmentStorage.getExecutor()),
                                        chunkedSegmentStorage.getExecutor());
                    }
                    return CompletableFuture.completedFuture(null);
                }, chunkedSegmentStorage.getExecutor())
                .thenApplyAsync(v -> state);
    }

    private CompletableFuture<State> findChunkForOffset(State state, MetadataTransaction txn) {

        state.currentChunkName = state.segmentMetadata.getFirstChunk();
        state.chunkToReadFrom = null;

        Preconditions.checkState(null != state.currentChunkName);

        state.bytesRemaining = length;
        state.currentBufferOffset = bufferOffset;
        state.currentOffset = offset;
        state.totalBytesRead = 0;

        // Find the first chunk that contains the data.
        state.startOffsetForCurrentChunk = state.segmentMetadata.getFirstChunkStartOffset();
        val readIndexTimer = new Timer();
        // Find the name of the chunk in the cached read index that is floor to required offset.
        val floorEntry = chunkedSegmentStorage.getReadIndexCache().findFloor(handle.getSegmentName(), offset);
        if (null != floorEntry) {
            state.startOffsetForCurrentChunk = floorEntry.getOffset();
            state.currentChunkName = floorEntry.getChunkName();
        }

        // Navigate to the chunk that contains the first byte of requested data.
        return Futures.loop(
                () -> state.currentChunkName != null && !state.isLoopExited,
                () -> txn.get(state.currentChunkName)
                        .thenApplyAsync(storageMetadata -> {
                            state.chunkToReadFrom = (ChunkMetadata) storageMetadata;
                                    return state;
                                })
                        .thenApplyAsync(state2 -> {
                            Preconditions.checkState(null != state.chunkToReadFrom, "chunkToReadFrom is null");
                            if (state.startOffsetForCurrentChunk <= state.currentOffset
                                    && state.startOffsetForCurrentChunk + state.chunkToReadFrom.getLength() > state.currentOffset) {
                                // we have found a chunk that contains first byte we want to read
                                log.debug("{} read - found chunk to read - segment={}, chunk={}, startOffset={}, length={}, readOffset={}.",
                                        chunkedSegmentStorage.getLogPrefix(), handle.getSegmentName(), state.chunkToReadFrom, state.startOffsetForCurrentChunk, state.chunkToReadFrom.getLength(), state.currentOffset);
                                state.isLoopExited = true;
                                return null;
                            }
                            state.currentChunkName = state.chunkToReadFrom.getNextChunk();
                            state.startOffsetForCurrentChunk += state.chunkToReadFrom.getLength();

                            // Update read index with newly visited chunk.
                            if (null != state.currentChunkName) {
                                chunkedSegmentStorage.getReadIndexCache().addIndexEntry(handle.getSegmentName(), state.currentChunkName, state.startOffsetForCurrentChunk);
                            }
                            cntScanned.incrementAndGet();
                            return null;
                        }, chunkedSegmentStorage.getExecutor())
                        .thenApplyAsync(state3 -> {
                            val elapsed = readIndexTimer.getElapsed();
                            SLTS_READ_INDEX_SCAN_LATENCY.reportSuccessEvent(elapsed);
                            log.debug("{} read - chunk lookup - segment={}, offset={}, scanned={}, latency={}.",
                                    chunkedSegmentStorage.getLogPrefix(), handle.getSegmentName(), offset, cntScanned.get(), elapsed.toMillis());
                            return null;
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor())
                .thenApplyAsync(v -> state);
    }

    private void checkState(State state) {
        chunkedSegmentStorage.checkSegmentExists(handle.getSegmentName(), state.segmentMetadata);

        state.segmentMetadata.checkInvariants();

        Preconditions.checkArgument(offset < state.segmentMetadata.getLength(), "Offset %s is beyond the last offset %s of the segment %s.",
                offset, state.segmentMetadata.getLength(), handle.getSegmentName());

        if (offset < state.segmentMetadata.getStartOffset()) {
            throw new CompletionException(new StreamSegmentTruncatedException(handle.getSegmentName(), state.segmentMetadata.getStartOffset(), offset));
        }
    }

    private void checkPreconditions(State state) {
        Preconditions.checkNotNull(handle, "handle");
        Preconditions.checkNotNull(buffer, "buffer");
        Preconditions.checkNotNull(handle.getSegmentName(), "streamSegmentName");

        Exceptions.checkArrayRange(bufferOffset, length, buffer.length, "bufferOffset", "length");

        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }
    }
}
