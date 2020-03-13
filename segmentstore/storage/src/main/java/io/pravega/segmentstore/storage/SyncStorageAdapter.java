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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.SegmentProperties;

import javax.annotation.concurrent.ThreadSafe;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adapter that asynchronously executes all operations by forwarding them to SyncStorage.
 *
 */
@ThreadSafe
public class SyncStorageAdapter implements Storage {
    //region Members

    private final SyncStorage syncStorage;
    private final Executor executor;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SyncStorageAdapter class.
     *
     * @param syncStorage A SyncStorage instance that will be called.
     * @param executor    An Executor for async operations.
     */
    public SyncStorageAdapter(SyncStorage syncStorage, Executor executor) {
        this.syncStorage = Preconditions.checkNotNull(syncStorage, "syncStorage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.syncStorage.close();
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    public void initialize(long containerEpoch) {
        this.syncStorage.initialize(containerEpoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return execute(() -> this.syncStorage.openWrite(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        return execute(() -> this.syncStorage.create(streamSegmentName, rollingPolicy));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return execute(() -> {
            this.syncStorage.write(handle, offset, data, length);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return execute(() -> {
            this.syncStorage.seal(handle);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return execute(() -> {
            this.syncStorage.concat(targetHandle, offset, sourceSegment);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return execute(() -> {
            this.syncStorage.delete(handle);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        return execute(() -> {
            this.syncStorage.truncate(handle, offset);
            return null;
        });
    }

    @Override
    public boolean supportsTruncation() {
        return this.syncStorage.supportsTruncation();
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return execute(() -> this.syncStorage.openRead(streamSegmentName));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return execute(() -> this.syncStorage.read(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return execute(() -> this.syncStorage.getStreamSegmentInfo(streamSegmentName));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return execute(() -> this.syncStorage.exists(streamSegmentName));
    }

    //endregion

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation     The function to execute.
     * @param <R>           Return type of the operation.
     * @return CompletableFuture<R> of the return type of the operation.
     */
    private <R> CompletableFuture<R> execute(Callable<R> operation) {
        Preconditions.checkState(!this.closed.get(), "SyncStorageAdapter is closed.");
        return CompletableFuture.supplyAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }
    //endregion
}
