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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base implementation of {@link ChunkStorage}.
 * It implements common functionality that can be used by derived classes.
 * Delegates to specific implementations by calling various abstract methods which must be overridden in derived classes.
 *
 * Below are minimum requirements that any implementation must provide.
 * Note that it is the responsibility of storage provider specific implementation to make sure following guarantees are provided even
 * though underlying storage may not provide all primitives or guarantees.
 * <ul>
 * <li>Once an operation is executed and acknowledged as successful then the effects must be permanent and consistent (as opposed to eventually consistent)</li>
 * <li>{@link ChunkStorage#create(String)}  and {@link ChunkStorage#delete(ChunkHandle)} are not idempotent.</li>
 * <li>{@link ChunkStorage#exists(String)} and {@link ChunkStorage#getInfo(String)} must reflect effects of most recent operation performed.</li>
 * </ul>
 *
 * There are a few different capabilities that ChunkStorage may provide.
 * <ul>
 * <li> Does {@link ChunkStorage} support appending to existing chunks?
 * This is indicated by {@link ChunkStorage#supportsAppend()}. For example S3 compatible Chunk Storage this would return false. </li>
 * <li> Does {@link ChunkStorage}  support for concatenating chunks? This is indicated by {@link ChunkStorage#supportsConcat()}.
 * If this is true then concat operation concat will be invoked otherwise append functionality is invoked.</li>
 * <li>In addition {@link ChunkStorage} may provide ability to truncate chunks at given offsets (either at front end or at tail end). This is indicated by {@link ChunkStorage#supportsTruncation()}. </li>
 * </ul>
 * There are some obvious constraints - If ChunkStorage supports concat but not natively then it must support append .
 *
 * For concats, {@link ChunkStorage} supports both native and append, ChunkedSegmentStorage will invoke appropriate method depending on size of target and source chunks. (Eg. ECS)
 *
 * The implementations in this repository are tested using following test suites.
 * <ul>
 * <li>SimpleStorageTests</li>
 * <li>ChunkedRollingStorageTests</li>
 * <li>ChunkStorageProviderTests</li>
 * <li>SystemJournalTests</li>
 * </ul>
 */
@Slf4j
@Beta
public abstract class AsyncBaseChunkStorage implements ChunkStorage {

    private final AtomicBoolean closed;

    private final Executor executor;

    /**
     * Constructor.
     *
     * @param executor  An Executor for async operations.
     */
    public AsyncBaseChunkStorage(Executor executor) {
        this.closed = new AtomicBoolean(false);
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on chunks.
     *
     * @return True or false.
     */
    @Override
    abstract public boolean supportsTruncation();

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on chunks.
     *
     * @return True or false.
     */
    @Override
    abstract public boolean supportsAppend();

    /**
     * Gets a value indicating whether this Storage implementation supports merge operation either natively or through appends.
     *
     * @return True or false.
     */
    @Override
    abstract public boolean supportsConcat();

    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the chunk to check.
     * @return True if the object exists, false otherwise.
     */
    @Override
    final public CompletableFuture<Boolean> exists(String chunkName) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            checkChunkName(chunkName);

            long traceId = LoggerHelpers.traceEnter(log, "exists", chunkName);
            // Call concrete implementation.
            val returnFuture =  checkExistsAsync(chunkName);
            returnFuture.thenApplyAsync(retValue -> {

                LoggerHelpers.traceLeave(log, "exists", traceId, chunkName);

                return retValue;
            });
            return returnFuture;
        });
    }

    /**
     * Creates a new chunk.
     *
     * @param chunkName Name of the chunk to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<ChunkHandle> create(String chunkName) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            checkChunkName(chunkName);

            long traceId = LoggerHelpers.traceEnter(log, "create", chunkName);
            Timer timer = new Timer();

            // Call concrete implementation.
            val returnFuture =   doCreateAsync(chunkName);
            returnFuture.thenApplyAsync(handle -> {

                // Record metrics.
                Duration elapsed = timer.getElapsed();
                ChunkStorageMetrics.CREATE_LATENCY.reportSuccessEvent(elapsed);
                ChunkStorageMetrics.CREATE_COUNT.inc();

                log.debug("Create - chunk={}, latency={}.", chunkName, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "create", traceId, chunkName);

                return handle;
            });

            return returnFuture;
        });
    }

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Void> delete(ChunkHandle handle) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            Preconditions.checkArgument(null != handle, "handle must not be null");
            checkChunkName(handle.getChunkName());
            Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly");

            long traceId = LoggerHelpers.traceEnter(log, "delete", handle.getChunkName());
            Timer timer = new Timer();

            // Call concrete implementation.
            val returnFuture = doDeleteAsync(handle);
            returnFuture.thenApplyAsync(v -> {

                // Record metrics.
                Duration elapsed = timer.getElapsed();
                ChunkStorageMetrics.DELETE_LATENCY.reportSuccessEvent(elapsed);
                ChunkStorageMetrics.DELETE_COUNT.inc();

                log.debug("Delete - chunk={}, latency={}.", handle.getChunkName(), elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "delete", traceId, handle.getChunkName());
                return null;
            });
            return returnFuture;
        });
    }

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    @Override
    final public CompletableFuture<ChunkHandle> openRead(String chunkName) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            checkChunkName(chunkName);

            long traceId = LoggerHelpers.traceEnter(log, "openRead", chunkName);

            // Call concrete implementation.
            val returnFuture = doOpenReadAsync(chunkName);
            returnFuture.thenApplyAsync(handle -> {

                LoggerHelpers.traceLeave(log, "openRead", traceId, chunkName);

                return handle;
            });
            return returnFuture;
        });
    }

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    @Override
    final public CompletableFuture<ChunkHandle> openWrite(String chunkName) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            checkChunkName(chunkName);

            long traceId = LoggerHelpers.traceEnter(log, "openWrite", chunkName);

            // Call concrete implementation.
            val returnFuture = doOpenWriteAsync(chunkName);
            returnFuture.thenApplyAsync(handle -> {

                LoggerHelpers.traceLeave(log, "openWrite", traceId, chunkName);

                return handle;
            });
            return returnFuture;
        });
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkInfo Information about the given chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    final public CompletableFuture<ChunkInfo> getInfo(String chunkName) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            checkChunkName(chunkName);
            long traceId = LoggerHelpers.traceEnter(log, "getInfo", chunkName);

            // Call concrete implementation.
            val returnFuture = doGetInfoAsync(chunkName);
            returnFuture.thenApplyAsync(info -> {

                LoggerHelpers.traceLeave(log, "getInfo", traceId, chunkName);

                return info;
            }, executor);
            return returnFuture;
        });
    }

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle       ChunkHandle of the chunk to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * throws ChunkStorageException     Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException  If argument is invalid.
     * throws IndexOutOfBoundsException If the index is out of bounds or offset is not a valid offset in the underlying file/object.
     */
    @Override
    final public CompletableFuture<Integer> read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            Preconditions.checkArgument(null != handle, "handle");
            checkChunkName(handle.getChunkName());
            Preconditions.checkArgument(null != buffer, "buffer");
            Preconditions.checkArgument(fromOffset >= 0, "fromOffset must be non-negative");
            Preconditions.checkArgument(length >= 0 && length <= buffer.length, "length");
            Preconditions.checkElementIndex(bufferOffset, buffer.length, "bufferOffset");

            long traceId = LoggerHelpers.traceEnter(log, "read", handle.getChunkName(), fromOffset, bufferOffset, length);
            Timer timer = new Timer();

            // Call concrete implementation.
            val returnFuture = doReadAsync(handle, fromOffset, length, buffer, bufferOffset);
            returnFuture.thenApplyAsync(bytesRead -> {

                Duration elapsed = timer.getElapsed();
                ChunkStorageMetrics.READ_LATENCY.reportSuccessEvent(elapsed);
                ChunkStorageMetrics.READ_BYTES.add(bytesRead);

                log.debug("Read - chunk={}, offset={}, bytesRead={}, latency={}.", handle.getChunkName(), fromOffset, length, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);

                return bytesRead;
            }, executor);
            return returnFuture;
        });
    }

    /**
     * Writes the given data to the underlying chunk.
     *
     * <ul>
     * <li>It is expected that in cases where it can not overwrite the existing data at given offset, the implementation should throw IndexOutOfBoundsException.</li>
     * For storage where underlying files/objects are immutable once written, the implementation should return false on {@link ChunkStorage#supportsAppend()}.
     * <li>In such cases only valid offset is 0.</li>
     * <li>For storages where underlying files/objects can only be appended but not overwritten, it must match actual current length of underlying file/object.</li>
     * <li>In all cases the offset can not be greater that actual current length of underlying file/object. </li>
     * </ul>
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return int Number of bytes written.
     * throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Integer> write(ChunkHandle handle, long offset, int length, InputStream data) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            Preconditions.checkArgument(null != handle, "handle must not be null");
            checkChunkName(handle.getChunkName());
            Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly");
            Preconditions.checkArgument(null != data, "data must not be null");
            Preconditions.checkArgument(offset >= 0, "offset must be non-negative");
            Preconditions.checkArgument(length >= 0, "length must be non-negative");
            if (!supportsAppend()) {
                Preconditions.checkArgument(offset == 0, "offset must be 0 because storage does not support appends.");
            }

            long traceId = LoggerHelpers.traceEnter(log, "write", handle.getChunkName(), offset, length);
            Timer timer = new Timer();

            // Call concrete implementation.
            val returnFuture = doWriteAsync(handle, offset, length, data);
            returnFuture.thenApplyAsync(bytesWritten -> {

                Duration elapsed = timer.getElapsed();

                ChunkStorageMetrics.WRITE_LATENCY.reportSuccessEvent(elapsed);
                ChunkStorageMetrics.WRITE_BYTES.add(bytesWritten);

                log.debug("Write - chunk={}, offset={}, bytesWritten={}, latency={}.", handle.getChunkName(), offset, length, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "read", traceId, bytesWritten);

                return bytesWritten;
            }, executor);
            return returnFuture;
        });
    }

    /**
     * Concatenates two or more chunks. The first chunk is concatenated to.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    final public CompletableFuture<Integer> concat(ConcatArgument[] chunks) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            checkConcatArgs(chunks);

            long traceId = LoggerHelpers.traceEnter(log, "concat", chunks[0].getName());
            Timer timer = new Timer();

            // Call concrete implementation.
            val returnFuture = doConcatAsync(chunks);

            returnFuture.thenApplyAsync(retValue -> {

                Duration elapsed = timer.getElapsed();
                log.debug("concat - target={}, latency={}.", chunks[0].getName(), elapsed.toMillis());

                ChunkStorageMetrics.CONCAT_LATENCY.reportSuccessEvent(elapsed);
                ChunkStorageMetrics.CONCAT_BYTES.add(retValue);
                ChunkStorageMetrics.CONCAT_COUNT.inc();
                ChunkStorageMetrics.LARGE_CONCAT_COUNT.inc();

                LoggerHelpers.traceLeave(log, "concat", traceId, chunks[0].getName());

                return retValue;
            }, executor);
            return returnFuture;
        });
    }

    private void checkConcatArgs(ConcatArgument[] chunks) {
        // Validate parameters
        Preconditions.checkArgument(null != chunks, "chunks must not be null");
        Preconditions.checkArgument(chunks.length >= 2, "There must be at least two chunks");

        Preconditions.checkArgument(null != chunks[0], "target chunk must not be null");
        Preconditions.checkArgument(chunks[0].getLength() >= 0, "target chunk lenth must be non negative.");
        checkChunkName(chunks[0].getName());

        for (int i = 1; i < chunks.length; i++) {
            Preconditions.checkArgument(null != chunks[i], "source chunk must not be null");
            checkChunkName(chunks[i].getName());
            Preconditions.checkArgument(chunks[i].getLength() >= 0, "source chunk lenth must be non negative.");
            Preconditions.checkArgument(!chunks[i].getName().equals(chunks[0].getName()), "source chunk is same as target");
            Preconditions.checkArgument(!chunks[i].getName().equals(chunks[i - 1].getName()), "duplicate chunk found");
        }
    }

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    final public CompletableFuture<Boolean> truncate(ChunkHandle handle, long offset) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            Preconditions.checkArgument(null != handle, "handle must not be null");
            checkChunkName(handle.getChunkName());
            Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly");
            Preconditions.checkArgument(offset >= 0, "offset must be non-negative");

            long traceId = LoggerHelpers.traceEnter(log, "truncate", handle.getChunkName());

            // Call concrete implementation.
            val returnFuture = doTruncateAsync(handle, offset);
            returnFuture.thenApplyAsync(retValue -> {

                LoggerHelpers.traceLeave(log, "truncate", traceId, handle.getChunkName());

                return retValue;
            }, executor);
            return returnFuture;
        });
    }

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle     ChunkHandle of the chunk.
     * @param isReadonly True if chunk is set to be readonly.
     * throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    final public CompletableFuture<Void> setReadOnly(ChunkHandle handle, boolean isReadonly) {
        return executeAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            // Validate parameters
            Preconditions.checkArgument(null != handle, "handle must not be null");
            checkChunkName(handle.getChunkName());

            long traceId = LoggerHelpers.traceEnter(log, "setReadOnly", handle.getChunkName());

            // Call concrete implementation.
            val returnFuture = doSetReadOnlyAsync(handle, isReadonly);
            returnFuture.thenApplyAsync(v -> {

                LoggerHelpers.traceLeave(log, "setReadOnly", traceId, handle.getChunkName());
                return null;
            }, executor);
            return returnFuture;
        });
    }

    /**
     * Closes.
     *
     */
    @Override
    public void close() {
        this.closed.set(true);
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkInfo Information about the given chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    abstract protected CompletableFuture<ChunkInfo> doGetInfoAsync(String chunkName);

    /**
     * Creates a new chunk.
     *
     * @param chunkName String name of the chunk to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    abstract protected CompletableFuture<ChunkHandle> doCreateAsync(String chunkName);

    /**
     * Determines whether named chunk exists in underlying storage.
     *
     * @param chunkName Name of the chunk to check.
     * @return True if the object exists, false otherwise.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    abstract protected CompletableFuture<Boolean> checkExistsAsync(String chunkName);

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    abstract protected CompletableFuture<Void> doDeleteAsync(ChunkHandle handle);

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    abstract protected CompletableFuture<ChunkHandle> doOpenReadAsync(String chunkName);

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException If argument is invalid.
     */
    abstract protected CompletableFuture<ChunkHandle> doOpenWriteAsync(String chunkName);

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle       ChunkHandle of the chunk to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * throws ChunkStorageException     Throws ChunkStorageException in case of I/O related exceptions.
     * throws IllegalArgumentException  If argument is invalid.
     * throws NullPointerException      If the parameter is null.
     * throws IndexOutOfBoundsException If the index is out of bounds.
     */
    abstract protected CompletableFuture<Integer> doReadAsync(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset);

    /**
     * Writes the given data to the chunk.
     *
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return int Number of bytes written.
     * throws ChunkStorageException     Throws ChunkStorageException in case of I/O related exceptions.
     * throws IndexOutOfBoundsException If the index is out of bounds.
     * throws IllegalArgumentException Throws IllegalArgumentException in case of invalid index.
     */
    abstract protected CompletableFuture<Integer> doWriteAsync(ChunkHandle handle, long offset, int length, InputStream data);

    /**
     * Concatenates two or more chunks using storage native functionality. (Eg. Multipart upload.)
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected CompletableFuture<Integer> doConcatAsync(ConcatArgument[] chunks);

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle     ChunkHandle of the chunk.
     * @param isReadOnly True if chunk is set to be readonly.
     * throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected CompletableFuture<Void> doSetReadOnlyAsync(ChunkHandle handle, boolean isReadOnly);

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    protected CompletableFuture<Boolean> doTruncateAsync(ChunkHandle handle, long offset) {
        throw new UnsupportedOperationException();
    }

    /**
     * Validate chunk name.
     * @param chunkName Chunk name.
     */
    protected void checkChunkName(String chunkName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(chunkName), "chunk name must not be null or empty");
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation The function to execute.
     * @param <R>       Return type of the operation.
     * @return CompletableFuture<R> of the return type of the operation.
     */
    protected  <R> CompletableFuture<R> execute(Callable<R> operation) {
        return CompletableFuture.supplyAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    private <R> CompletableFuture<R> executeAsync(Callable<CompletableFuture<R>> operation) {
        return CompletableFuture.completedFuture(null).thenComposeAsync(v -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (CompletionException e) {
                throw new CompletionException(Exceptions.unwrap(e));
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }
}
