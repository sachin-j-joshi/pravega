/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import io.pravega.shared.NameUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implements storage for segments using {@link ChunkStorage} and {@link ChunkMetadataStore}.
 * The metadata about the segments is stored in metadataStore using two types of records {@link SegmentMetadata} and {@link ChunkMetadata}.
 * Any changes to layout must be made inside a {@link MetadataTransaction} which will atomically change the records upon
 * {@link MetadataTransaction#commit()}.
 * Detailed design is documented here https://github.com/pravega/pravega/wiki/PDP-34:-Simplified-Tier-2
 */
@Slf4j
@Beta
public class ChunkedSegmentStorage implements Storage {
    /**
     * Configuration options for this ChunkedSegmentStorage instance.
     */
    @Getter
    private final ChunkedSegmentStorageConfig config;

    /**
     * Metadata store containing all storage data.
     * Initialized by segment container via {@link ChunkedSegmentStorage#bootstrap(int, ChunkMetadataStore)}.
     */
    @Getter
    private ChunkMetadataStore metadataStore;

    /**
     * Underlying {@link ChunkStorage} to use to read and write data.
     */
    @Getter
    private final ChunkStorage chunkStorage;

    /**
     * Storage executor object.
     */
    @Getter
    private final Executor executor;

    /**
     * Tracks whether this instance is closed or not.
     */
    private final AtomicBoolean closed;

    /**
     * Current epoch of the {@link Storage} instance.
     * Initialized by segment container via {@link ChunkedSegmentStorage#initialize(long)}.
     */
    @Getter
    private long epoch;

    /**
     * Id of the current Container.
     * Initialized by segment container via {@link ChunkedSegmentStorage#bootstrap(int, ChunkMetadataStore)}.
     */
    @Getter
    private int containerId;

    /**
     * {@link SystemJournal} that logs all changes to system segment layout so that they can be are used during system bootstrap.
     */
    @Getter
    private SystemJournal systemJournal;

    /**
     * {@link ReadIndexCache} that has index of chunks by start offset
     */
    private final ReadIndexCache readIndexCache;

    /**
     * List of garbage chunks.
     */
    private final List<String> garbageChunks = new ArrayList<>();

    /**
     * Prefix string to use for logging.
     */
    private String logPrefix;

    /**
     * Creates a new instance of the {@link ChunkedSegmentStorage} class.
     *
     * @param chunkStorage ChunkStorage instance.
     * @param executor     An Executor for async operations.
     * @param config       Configuration options for this ChunkedSegmentStorage instance.
     */
    public ChunkedSegmentStorage(ChunkStorage chunkStorage, Executor executor, ChunkedSegmentStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readIndexCache = new ReadIndexCache(config.getMaxIndexedSegments(),
                config.getMaxIndexedChunksPerSegment(),
                config.getMaxIndexedChunks());
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Creates a new instance of the ChunkedSegmentStorage class.
     *
     * @param chunkStorage  ChunkStorage instance.
     * @param metadataStore Metadata store.
     * @param executor      An Executor for async operations.
     * @param config        Configuration options for this ChunkedSegmentStorage instance.
     */
    public ChunkedSegmentStorage(ChunkStorage chunkStorage, ChunkMetadataStore metadataStore, Executor executor, ChunkedSegmentStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readIndexCache = new ReadIndexCache(config.getMaxIndexedSegments(),
                config.getMaxIndexedChunksPerSegment(),
                config.getMaxIndexedChunks());
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Initializes the ChunkedSegmentStorage and bootstrap the metadata about storage metadata segments by reading and processing the journal.
     *
     * @param metadataStore Metadata store.
     * @param containerId   container id.
     * @throws Exception In case of any errors.
     */
    public CompletableFuture<Void> bootstrap(int containerId, ChunkMetadataStore metadataStore) throws Exception {
        this.containerId = containerId;
        this.logPrefix = String.format("ChunkedSegmentStorage[%d]", containerId);
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.systemJournal = new SystemJournal(containerId,
                epoch,
                chunkStorage,
                metadataStore,
                config);

        // Now bootstrap
        log.info("{} STORAGE BOOT: Started.", logPrefix);
        return this.systemJournal.bootstrap().thenApplyAsync(v -> {
            log.info("{} STORAGE BOOT: Ended.", logPrefix);
            return null;
        }, executor);
    }

    @Override
    public void initialize(long containerEpoch) {
        this.epoch = containerEpoch;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            return tryWith(metadataStore.beginTransaction(streamSegmentName),
                    txn -> txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                segmentMetadata.checkInvariants();
                                // This segment was created by an older segment store. Need to start a fresh new chunk.
                                CompletableFuture<Void> f = CompletableFuture.completedFuture(null);
                                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                                    log.debug("{} openWrite - Segment needs ownership change - segment={}.", logPrefix, segmentMetadata.getName());
                                    f = claimOwnership(txn, segmentMetadata)
                                            .exceptionally(e -> {
                                                val ex = Exceptions.unwrap(e);
                                                if (ex instanceof StorageMetadataWritesFencedOutException) {
                                                    throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
                                                }
                                                throw new CompletionException(ex);
                                            });
                                }
                                return f.thenApplyAsync(v -> {
                                    // If created by newer instance then abort.
                                    checkOwnership(streamSegmentName, segmentMetadata);

                                    // This instance is the owner, return a handle.
                                    val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                                    LoggerHelpers.traceLeave(log, "openWrite", traceId, retValue);
                                    return retValue;
                                }, executor);
                            }, executor),
                    executor);
        });
    }

    /**
     * Checks ownership and adjusts the length of the segment if required.
     *
     * @param txn             Active {@link MetadataTransaction}.
     * @param segmentMetadata {@link SegmentMetadata} for the segment to change ownership for.
     *                        throws ChunkStorageException    In case of any chunk storage related errors.
     *                        throws StorageMetadataException In case of any chunk metadata store related errors.
     */
    private CompletableFuture<Void> claimOwnership(MetadataTransaction txn, SegmentMetadata segmentMetadata) {
        // Get the last chunk
        String lastChunkName = segmentMetadata.getLastChunk();
        CompletableFuture<Boolean> f = CompletableFuture.completedFuture(true);
        if (null != lastChunkName) {
            f = txn.get(lastChunkName)
                    .thenComposeAsync(storageMetadata -> {
                        ChunkMetadata lastChunk = (ChunkMetadata) storageMetadata;
                        log.debug("{} claimOwnership - current last chunk - segment={}, last chunk={}, Length={}.",
                                logPrefix,
                                segmentMetadata.getName(),
                                lastChunk.getName(),
                                lastChunk.getLength());
                        return chunkStorage.getInfo(lastChunkName)
                                .thenComposeAsync(chunkInfo -> {
                                    Preconditions.checkState(chunkInfo != null);
                                    Preconditions.checkState(lastChunk != null);
                                    // Adjust its length;
                                    if (chunkInfo.getLength() != lastChunk.getLength()) {
                                        Preconditions.checkState(chunkInfo.getLength() > lastChunk.getLength());
                                        // Whatever length you see right now is the final "sealed" length of the last chunk.
                                        lastChunk.setLength(chunkInfo.getLength());
                                        segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + lastChunk.getLength());
                                        txn.update(lastChunk);
                                        log.debug("{} claimOwnership - Length of last chunk adjusted - segment={}, last chunk={}, Length={}.",
                                                logPrefix,
                                                segmentMetadata.getName(),
                                                lastChunk.getName(),
                                                chunkInfo.getLength());
                                    }
                                    return CompletableFuture.completedFuture(true);
                                }, executor)
                                .exceptionally(e -> {
                                    val ex = Exceptions.unwrap(e);
                                    if (ex instanceof ChunkNotFoundException) {
                                        // This probably means that this instance is fenced out and newer instance truncated this segment.
                                        // Try a commit of unmodified data to fail fast.
                                        log.debug("{} claimOwnership - Last chunk was missing, failing fast - segment={}, last chunk={}.",
                                                logPrefix,
                                                segmentMetadata.getName(),
                                                lastChunk.getName());
                                        txn.update(segmentMetadata);
                                        return false;
                                    }
                                    throw new CompletionException(ex);
                                });

                    }, executor);
        }

        return f.thenComposeAsync(shouldChange -> {
            // Claim ownership.
            // This is safe because the previous instance is definitely not an owner anymore. (even if this instance is no more owner)
            // If this instance is no more owner, then transaction commit will fail.So it is still safe.
            if (shouldChange) {
                segmentMetadata.setOwnerEpoch(this.epoch);
                segmentMetadata.setOwnershipChanged(true);
            }
            // Update and commit
            // If This instance is fenced this update will fail.
            txn.update(segmentMetadata);
            return txn.commit();
        }, executor);
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName, rollingPolicy);
            Timer timer = new Timer();

            return tryWith(metadataStore.beginTransaction(streamSegmentName), txn -> {
                // Retrieve metadata and make sure it does not exist.
                return txn.get(streamSegmentName)
                        .thenComposeAsync(storageMetadata -> {
                            SegmentMetadata oldSegmentMetadata = (SegmentMetadata) storageMetadata;
                            if (null != oldSegmentMetadata) {
                                throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
                            }

                            // Create a new record.
                            SegmentMetadata newSegmentMetatadata = SegmentMetadata.builder()
                                    .name(streamSegmentName)
                                    .maxRollinglength(rollingPolicy.getMaxLength() == 0 ? SegmentRollingPolicy.NO_ROLLING.getMaxLength() : rollingPolicy.getMaxLength())
                                    .ownerEpoch(this.epoch)
                                    .build();

                            newSegmentMetatadata.setActive(true);
                            txn.create(newSegmentMetatadata);
                            // commit.
                            return txn.commit()
                                    .thenApplyAsync(v -> {

                                        val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                                        Duration elapsed = timer.getElapsed();
                                        log.debug("{} create - segment={}, rollingPolicy={}, latency={}.", logPrefix, streamSegmentName, rollingPolicy, elapsed.toMillis());
                                        LoggerHelpers.traceLeave(log, "create", traceId, retValue);
                                        return retValue;
                                    }, executor)
                                    .handleAsync((v, e) -> {
                                        handleException(streamSegmentName, e);
                                        return v;
                                    }, executor);
                        }, executor);
            }, executor);
        });
    }

    private void handleException(String streamSegmentName, Throwable e) {
        if (null != e) {
            val ex = Exceptions.unwrap(e);
            if (ex instanceof StorageMetadataWritesFencedOutException) {
                throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
            }
            throw new CompletionException(ex);
        }
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        checkInitialized();
        return executeAsync(new WriteOperation(this, handle, offset, data, length));
    }

    /**
     * Gets whether given segment is a critical storage system segment.
     *
     * @param segmentMetadata Meatadata for the segment.
     * @return True if this is a storage system segment.
     */
    private boolean isStorageSystemSegment(SegmentMetadata segmentMetadata) {
        return null != systemJournal && segmentMetadata.isStorageSystemSegment();
    }

    /**
     * Delete the garbage chunks.
     *
     * @param chunksTodelete List of chunks to delete.
     */
    private CompletableFuture<Void> collectGarbage(Collection<String> chunksTodelete) {
        CompletableFuture[] futures = new CompletableFuture[chunksTodelete.size()];
        int i = 0;
        for (val chunkTodelete : chunksTodelete) {
            futures[i++] = chunkStorage.openWrite(chunkTodelete)
                    .thenComposeAsync(chunkStorage::delete, executor)
                    .thenRunAsync(() -> log.debug("{} collectGarbage - deleted chunk={}.", logPrefix, chunkTodelete), executor)
                    .exceptionally(e -> {
                        val ex = Exceptions.unwrap(e);
                        if (ex instanceof ChunkNotFoundException) {
                            log.debug("{} collectGarbage - Could not delete garbage chunk {}.", logPrefix, chunkTodelete);
                        } else {
                            log.warn("{} collectGarbage - Could not delete garbage chunk {}.", logPrefix, chunkTodelete);
                            // Add it to garbage chunks.
                            synchronized (garbageChunks) {
                                garbageChunks.add(chunkTodelete);
                            }
                        }
                        return null;
                    });
        }
        return CompletableFuture.allOf(futures);
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
            Preconditions.checkNotNull(handle, "handle");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");

            return tryWith(metadataStore.beginTransaction(handle.getSegmentName()), txn ->
                    txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                // Validate preconditions.
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                checkOwnership(streamSegmentName, segmentMetadata);

                                // seal if it is not already sealed.
                                if (!segmentMetadata.isSealed()) {
                                    segmentMetadata.setSealed(true);
                                    txn.update(segmentMetadata);
                                    return txn.commit();
                                }
                                return CompletableFuture.completedFuture(null);
                            }, executor)
                            .thenRunAsync(() -> {
                                log.debug("{} seal - segment={}.", logPrefix, handle.getSegmentName());
                                LoggerHelpers.traceLeave(log, "seal", traceId, handle);
                            }, executor)
                            .exceptionally(e -> {
                                val ex = Exceptions.unwrap(e);
                                if (ex instanceof StorageMetadataWritesFencedOutException) {
                                    throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
                                }
                                throw new CompletionException(ex);
                            }), executor);
        });
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        checkInitialized();
        return executeAsync(new ConcatOperation(this, targetHandle, offset, sourceSegment));
    }

    private boolean shouldAppend() {
        return chunkStorage.supportsAppend() && config.isAppendEnabled();
    }

    /**
     * Defragments the list of chunks for a given segment.
     * It finds eligible consecutive chunks that can be merged together.
     * The sublist such elgible chunks is replaced with single new chunk record corresponding to new large chunk.
     * Conceptually this is like deleting nodes from middle of the list of chunks.
     *
     * @param txn             Active {@link MetadataTransaction}.
     * @param segmentMetadata {@link SegmentMetadata} for the segment to defrag.
     * @param startChunkName  Name of the first chunk to start defragmentation.
     * @param lastChunkName   Name of the last chunk before which to stop defragmentation. (last chunk is not concatenated).
     * @param chunksToDelete  List of chunks to which names of chunks to be deleted are added. It is the responsibility
     *                        of caller to garbage collect these chunks.
     *                        throws ChunkStorageException    In case of any chunk storage related errors.
     *                        throws StorageMetadataException In case of any chunk metadata store related errors.
     */
    private CompletableFuture<Void> defrag(MetadataTransaction txn, SegmentMetadata segmentMetadata,
                                           String startChunkName,
                                           String lastChunkName,
                                           ArrayList<String> chunksToDelete) {
        return new DefragmentOperation(this, txn, segmentMetadata, startChunkName, lastChunkName, chunksToDelete).call();
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
            Timer timer = new Timer();

            String streamSegmentName = handle.getSegmentName();
            return tryWith(metadataStore.beginTransaction(streamSegmentName), txn -> txn.get(streamSegmentName)
                    .thenComposeAsync(storageMetadata -> {
                        SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                        // Check preconditions
                        checkSegmentExists(streamSegmentName, segmentMetadata);
                        checkOwnership(streamSegmentName, segmentMetadata);

                        segmentMetadata.setActive(false);

                        // Delete chunks
                        ArrayList<String> chunksToDelete = new ArrayList<>();
                        return new ChunkIterator(txn, segmentMetadata)
                                .forEach((metadata, name) -> {
                                    txn.delete(name);
                                    chunksToDelete.add(name);
                                })
                                .thenRunAsync(() -> txn.delete(streamSegmentName), executor)
                                .thenComposeAsync(v ->
                                        txn.commit()
                                                .thenComposeAsync(vv -> {
                                                    // Collect garbage.
                                                    return collectGarbage(chunksToDelete);
                                                }, executor)
                                                .thenRunAsync(() -> {
                                                    // Update the read index.
                                                    readIndexCache.remove(streamSegmentName);

                                                    Duration elapsed = timer.getElapsed();
                                                    log.debug("{} delete - segment={}, latency={}.", logPrefix, handle.getSegmentName(), elapsed.toMillis());
                                                    LoggerHelpers.traceLeave(log, "delete", traceId, handle);
                                                }, executor)
                                                .exceptionally(e -> {
                                                    val ex = Exceptions.unwrap(e);
                                                    if (ex instanceof StorageMetadataWritesFencedOutException) {
                                                        throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
                                                    }
                                                    throw new CompletionException(ex);
                                                }), executor);
                    }, executor), executor);
        });
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        checkInitialized();
        return executeAsync(new TruncateOperation(this, handle, offset));
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    @Override
    public Iterator<SegmentProperties> listSegments() {
        throw new UnsupportedOperationException("listSegments is not yet supported");
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
            // Validate preconditions and return handle.
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            return tryWith(metadataStore.beginTransaction(streamSegmentName), txn ->
                            txn.get(streamSegmentName).thenComposeAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                segmentMetadata.checkInvariants();
                                // This segment was created by an older segment store. Then claim ownership and adjust length.
                                CompletableFuture<Void> f = CompletableFuture.completedFuture(null);
                                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                                    log.debug("{} openRead - Segment needs ownership change. segment={}.", logPrefix, segmentMetadata.getName());
                                    // In case of a failover, length recorded in metadata will be lagging behind its actual length in the storage.
                                    // This can happen with lazy commits that were still not committed at the time of failover.
                                    f = claimOwnership(txn, segmentMetadata);
                                }
                                return f.thenApplyAsync(v -> {
                                    val retValue = SegmentStorageHandle.readHandle(streamSegmentName);
                                    LoggerHelpers.traceLeave(log, "openRead", traceId, retValue);
                                    return retValue;
                                }, executor);
                            }, executor),
                    executor);
        });
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        checkInitialized();
        return executeAsync(new ReadOperation(this, handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            return tryWith(metadataStore.beginTransaction(streamSegmentName), txn ->
                    txn.get(streamSegmentName)
                            .thenApplyAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                if (null == segmentMetadata) {
                                    throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                                }
                                segmentMetadata.checkInvariants();

                                val retValue = StreamSegmentInformation.builder()
                                        .name(streamSegmentName)
                                        .sealed(segmentMetadata.isSealed())
                                        .length(segmentMetadata.getLength())
                                        .startOffset(segmentMetadata.getStartOffset())
                                        .lastModified(new ImmutableDate(segmentMetadata.getLastModified()))
                                        .build();
                                LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, retValue);
                                return retValue;
                            }, executor), executor);
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        checkInitialized();
        return executeAsync(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            return tryWith(metadataStore.beginTransaction(streamSegmentName),
                    txn -> txn.get(streamSegmentName)
                            .thenApplyAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                val retValue = segmentMetadata != null && segmentMetadata.isActive();
                                LoggerHelpers.traceLeave(log, "exists", traceId, retValue);
                                return retValue;
                            }, executor),
                    executor);
        });
    }

    @Override
    public void close() {
        try {
            if (null != this.metadataStore) {
                this.metadataStore.close();
            }
        } catch (Exception e) {
            log.warn("Error during close", e);
        }
        this.closed.set(true);
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation The function to execute.
     * @param <R>       Return type of the operation.
     * @return CompletableFuture<R> of the return type of the operation.
     */
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

    private static <T extends AutoCloseable, R> CompletableFuture<R> tryWith(T closeable, Function<T, CompletableFuture<R>> function, Executor executor) {
        return function.apply(closeable)
                    .thenApplyAsync(v -> {
                        try {
                            closeable.close();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                        return v;
                    }, executor);
    }

    private void checkSegmentExists(String streamSegmentName, SegmentMetadata segmentMetadata) {
        if (null == segmentMetadata || !segmentMetadata.isActive()) {
            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
        }
    }

    private void checkOwnership(String streamSegmentName, SegmentMetadata segmentMetadata) {
        if (segmentMetadata.getOwnerEpoch() > this.epoch) {
            throw new CompletionException(new StorageNotPrimaryException(streamSegmentName));
        }
    }

    private void checkNotSealed(String streamSegmentName, SegmentMetadata segmentMetadata) {
        if (segmentMetadata.isSealed()) {
            throw new CompletionException(new StreamSegmentSealedException(streamSegmentName));
        }
    }

    private void checkInitialized() {
        Preconditions.checkState(null != this.metadataStore);
        Preconditions.checkState(0 != this.epoch);
        Preconditions.checkState(!closed.get());
    }

    private class ChunkIterator {
        private final MetadataTransaction txn;
        private String currentChunkName;
        private ChunkMetadata currentMetadata;

        ChunkIterator(MetadataTransaction txn, SegmentMetadata segmentMetadata) {
            this.txn = txn;
            currentChunkName = segmentMetadata.getFirstChunk();
        }

        public CompletableFuture<Void> forEach(BiConsumer<ChunkMetadata, String> consumer) {
            return Futures.loop(
                    () -> currentChunkName != null,
                    () -> txn.get(currentChunkName)
                            .thenApplyAsync(storageMetadata -> {
                                currentMetadata = (ChunkMetadata) storageMetadata;
                                consumer.accept(currentMetadata, currentChunkName);
                                // Move next
                                currentChunkName = currentMetadata.getNextChunk();
                                return null;
                            }, executor),
                    executor);
        }
    }

    private static class TruncateOperation implements Callable<CompletableFuture<Void>> {
        private SegmentHandle handle;
        private long offset;
        private ChunkedSegmentStorage chunkedSegmentStorage;

        private String currentChunkName;
        private ChunkMetadata currentMetadata;
        private long oldLength;
        private long startOffset;
        private ArrayList<String> chunksToDelete = new ArrayList<>();
        private SegmentMetadata segmentMetadata;
        private String streamSegmentName;

        private boolean isLoopExited;
        private long traceId;
        private Timer timer;

        TruncateOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset) {
            this.handle = handle;
            this.offset = offset;
            this.chunkedSegmentStorage = chunkedSegmentStorage;
        }

        public CompletableFuture<Void> call() {
            traceId = LoggerHelpers.traceEnter(log, "truncate", handle, offset);
            timer = new Timer();

            checkPreconditions();

            streamSegmentName = handle.getSegmentName();
            return tryWith(chunkedSegmentStorage.metadataStore.beginTransaction(streamSegmentName), txn ->
                    txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                segmentMetadata = (SegmentMetadata) storageMetadata;
                                // Check preconditions
                                checkPreconditions(streamSegmentName, segmentMetadata);

                                if (segmentMetadata.getStartOffset() == offset) {
                                    // Nothing to do
                                    return CompletableFuture.completedFuture(null);
                                }

                                return updateFirstChunk(txn)
                                        .thenComposeAsync(v -> {
                                            deleteChunks(txn);

                                            txn.update(segmentMetadata);

                                            // Check invariants.
                                            Preconditions.checkState(segmentMetadata.getLength() == oldLength, "truncate should not change segment length");
                                            segmentMetadata.checkInvariants();

                                            // Finally commit.
                                            return commit(txn)
                                                    .handleAsync(this::handleException, chunkedSegmentStorage.executor)
                                                    .thenComposeAsync(vv ->
                                                                    chunkedSegmentStorage.collectGarbage(chunksToDelete).thenApplyAsync(vvv -> {
                                                                        postCommit();
                                                                        return null;
                                                                    }, chunkedSegmentStorage.executor),
                                                            chunkedSegmentStorage.executor);
                                        }, chunkedSegmentStorage.executor);
                            }, chunkedSegmentStorage.executor), chunkedSegmentStorage.executor);
        }

        private void postCommit() {
            // Update the read index by removing all entries below truncate offset.
            chunkedSegmentStorage.readIndexCache.truncateReadIndex(streamSegmentName, segmentMetadata.getStartOffset());

            logEnd();
        }

        private void logEnd() {
            Duration elapsed = timer.getElapsed();
            log.debug("{} truncate - segment={}, offset={}, latency={}.", chunkedSegmentStorage.logPrefix, handle.getSegmentName(), offset, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "truncate", traceId, handle, offset);
        }

        private Void handleException(Void value, Throwable e) {
            if (null != e) {
                val ex = Exceptions.unwrap(e);
                if (ex instanceof StorageMetadataWritesFencedOutException) {
                    throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
                }
                throw new CompletionException(ex);
            }
            return value;
        }

        private CompletableFuture<Void> commit(MetadataTransaction txn) {
            // Commit system logs.
            if (chunkedSegmentStorage.isStorageSystemSegment(segmentMetadata)) {
                val finalStartOffset = startOffset;
                txn.setExternalCommitStep(() -> {
                    chunkedSegmentStorage.systemJournal.commitRecord(
                            SystemJournal.TruncationRecord.builder()
                                    .segmentName(streamSegmentName)
                                    .offset(offset)
                                    .firstChunkName(segmentMetadata.getFirstChunk())
                                    .startOffset(finalStartOffset)
                                    .build());
                    return null;
                });
            }

            // Finally commit.
            return txn.commit();
        }

        private CompletableFuture<Void> updateFirstChunk(MetadataTransaction txn) {
            currentChunkName = segmentMetadata.getFirstChunk();
            oldLength = segmentMetadata.getLength();
            startOffset = segmentMetadata.getFirstChunkStartOffset();
            return Futures.loop(
                    () -> currentChunkName != null && !isLoopExited,
                    () -> txn.get(currentChunkName)
                            .thenApplyAsync(storageMetadata -> {
                                currentMetadata = (ChunkMetadata) storageMetadata;
                                Preconditions.checkState(null != currentMetadata, "currentMetadata is null.");

                                // If for given chunk start <= offset < end  then we have found the chunk that will be the first chunk.
                                if ((startOffset <= offset) && (startOffset + currentMetadata.getLength() > offset)) {
                                    isLoopExited = true;
                                    return null;
                                }

                                startOffset += currentMetadata.getLength();
                                chunksToDelete.add(currentMetadata.getName());
                                segmentMetadata.decrementChunkCount();

                                // move to next chunk
                                currentChunkName = currentMetadata.getNextChunk();
                                return null;
                            }, chunkedSegmentStorage.executor),
                    chunkedSegmentStorage.executor
            ).thenApplyAsync(v -> {
                segmentMetadata.setFirstChunk(currentChunkName);
                segmentMetadata.setStartOffset(offset);
                segmentMetadata.setFirstChunkStartOffset(startOffset);
                return null;
            }, chunkedSegmentStorage.executor);
        }

        private void deleteChunks(MetadataTransaction txn) {
            for (String toDelete : chunksToDelete) {
                txn.delete(toDelete);
                // Adjust last chunk if required.
                if (toDelete.equals(segmentMetadata.getLastChunk())) {
                    segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());
                    segmentMetadata.setLastChunk(null);
                }
            }
        }

        private void checkPreconditions(String streamSegmentName, SegmentMetadata segmentMetadata) {
            chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);
            chunkedSegmentStorage.checkNotSealed(streamSegmentName, segmentMetadata);
            chunkedSegmentStorage.checkOwnership(streamSegmentName, segmentMetadata);

            if (segmentMetadata.getLength() < offset || segmentMetadata.getStartOffset() > offset) {
                throw new IllegalArgumentException(String.format("offset %d is outside of valid range [%d, %d) for segment %s",
                        offset, segmentMetadata.getStartOffset(), segmentMetadata.getLength(), streamSegmentName));
            }
        }

        private void checkPreconditions() {
            Preconditions.checkArgument(null != handle, "handle");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");
            Preconditions.checkArgument(offset >= 0, "offset");
        }
    }

    private static class ReadOperation implements Callable<CompletableFuture<Integer>> {
        private long traceId;
        private Timer timer;
        private SegmentHandle handle;
        private long offset;
        private byte[] buffer;
        private int bufferOffset;
        private int length;
        private ChunkedSegmentStorage chunkedSegmentStorage;
        private String streamSegmentName;
        private SegmentMetadata segmentMetadata;
        private int bytesRemaining;
        private int currentBufferOffset;
        private long currentOffset;
        private int totalBytesRead = 0;
        private long startOffsetForCurrentChunk;
        private String currentChunkName;
        private ChunkMetadata chunkToReadFrom = null;
        private boolean isLoopExited;
        private int cntScanned = 0;
        private int bytesToRead;

        ReadOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) {
            this.handle = handle;
            this.offset = offset;
            this.buffer = buffer;
            this.bufferOffset = bufferOffset;
            this.length = length;
            this.chunkedSegmentStorage = chunkedSegmentStorage;
        }

        public CompletableFuture<Integer> call() {
            traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
            timer = new Timer();

            // Validate preconditions.
            checkPreconditions();
            streamSegmentName = handle.getSegmentName();
            return tryWith(chunkedSegmentStorage.metadataStore.beginTransaction(streamSegmentName),
                    txn -> txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                segmentMetadata = (SegmentMetadata) storageMetadata;

                                // Validate preconditions.
                                checkState();

                                if (length == 0) {
                                    return CompletableFuture.completedFuture(0);
                                }

                                return findChunkForOffset(txn)
                                        .thenComposeAsync(v -> {
                                            // Now read.
                                            return readData(txn);
                                        }, chunkedSegmentStorage.executor)
                                        .thenApplyAsync(v -> {
                                            logEnd();
                                            return totalBytesRead;
                                        }, chunkedSegmentStorage.executor);
                            }, chunkedSegmentStorage.executor),
                    chunkedSegmentStorage.executor);
        }

        private void logEnd() {
            Duration elapsed = timer.getElapsed();
            log.debug("{} read - segment={}, offset={}, bytesRead={}, latency={}.", chunkedSegmentStorage.logPrefix, handle.getSegmentName(), offset, totalBytesRead, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, totalBytesRead);
        }

        private CompletableFuture<Void> readData(MetadataTransaction txn) {
            return Futures.loop(
                    () -> bytesRemaining > 0 && null != currentChunkName,
                    () -> {
                        bytesToRead = Math.min(bytesRemaining, Math.toIntExact(chunkToReadFrom.getLength() - (currentOffset - startOffsetForCurrentChunk)));
                        if (currentOffset >= startOffsetForCurrentChunk + chunkToReadFrom.getLength()) {
                            // The current chunk is over. Move to the next one.
                            currentChunkName = chunkToReadFrom.getNextChunk();
                            if (null != currentChunkName) {
                                startOffsetForCurrentChunk += chunkToReadFrom.getLength();
                                return txn.get(currentChunkName)
                                        .thenApplyAsync(storageMetadata -> {
                                            chunkToReadFrom = (ChunkMetadata) storageMetadata;
                                            log.debug("{} read - reading from next chunk - segment={}, chunk={}", chunkedSegmentStorage.logPrefix, streamSegmentName, chunkToReadFrom);
                                            return null;
                                        }, chunkedSegmentStorage.executor);
                            }
                        } else {
                            Preconditions.checkState(bytesToRead != 0, "bytesToRead is 0");
                            // Read data from the chunk.
                            return chunkedSegmentStorage.chunkStorage.openRead(chunkToReadFrom.getName())
                                    .thenComposeAsync(chunkHandle ->
                                            chunkedSegmentStorage.chunkStorage.read(chunkHandle,
                                                    currentOffset - startOffsetForCurrentChunk,
                                                    bytesToRead,
                                                    buffer,
                                                    currentBufferOffset)
                                                    .thenApplyAsync(bytesRead -> {
                                                        bytesRemaining -= bytesRead;
                                                        currentOffset += bytesRead;
                                                        currentBufferOffset += bytesRead;
                                                        totalBytesRead += bytesRead;
                                                        return null;
                                                    }, chunkedSegmentStorage.executor), chunkedSegmentStorage.executor);
                        }
                        return CompletableFuture.completedFuture(null);
                    }, chunkedSegmentStorage.executor);
        }

        private CompletableFuture<Void> findChunkForOffset(MetadataTransaction txn) {

            currentChunkName = segmentMetadata.getFirstChunk();
            chunkToReadFrom = null;

            Preconditions.checkState(null != currentChunkName);

            bytesRemaining = length;
            currentBufferOffset = bufferOffset;
            currentOffset = offset;
            totalBytesRead = 0;

            // Find the first chunk that contains the data.
            startOffsetForCurrentChunk = segmentMetadata.getFirstChunkStartOffset();
            Timer timer1 = new Timer();
            // Find the name of the chunk in the cached read index that is floor to required offset.
            val floorEntry = chunkedSegmentStorage.readIndexCache.findFloor(streamSegmentName, offset);
            if (null != floorEntry) {
                startOffsetForCurrentChunk = floorEntry.getOffset();
                currentChunkName = floorEntry.getChunkName();
            }

            // Navigate to the chunk that contains the first byte of requested data.
            return Futures.loop(
                    () -> currentChunkName != null && !isLoopExited,
                    () -> txn.get(currentChunkName)
                            .thenApplyAsync(storageMetadata -> {
                                chunkToReadFrom = (ChunkMetadata) storageMetadata;
                                Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null");
                                if (startOffsetForCurrentChunk <= currentOffset
                                        && startOffsetForCurrentChunk + chunkToReadFrom.getLength() > currentOffset) {
                                    // we have found a chunk that contains first byte we want to read
                                    log.debug("{} read - found chunk to read - segment={}, chunk={}, startOffset={}, length={}, readOffset={}.",
                                            chunkedSegmentStorage.logPrefix, streamSegmentName, chunkToReadFrom, startOffsetForCurrentChunk, chunkToReadFrom.getLength(), currentOffset);
                                    isLoopExited = true;
                                    return null;
                                }
                                currentChunkName = chunkToReadFrom.getNextChunk();
                                startOffsetForCurrentChunk += chunkToReadFrom.getLength();

                                // Update read index with newly visited chunk.
                                if (null != currentChunkName) {
                                    chunkedSegmentStorage.readIndexCache.addIndexEntry(streamSegmentName, currentChunkName, startOffsetForCurrentChunk);
                                }
                                cntScanned++;
                                return null;
                            }, chunkedSegmentStorage.executor)
                            .thenApplyAsync(v -> {
                                log.debug("{} read - chunk lookup - segment={}, offset={}, scanned={}, latency={}.",
                                    chunkedSegmentStorage.logPrefix, handle.getSegmentName(), offset, cntScanned, timer1.getElapsed().toMillis());
                                return null;
                            }, chunkedSegmentStorage.executor),
                    chunkedSegmentStorage.executor);
        }

        private void checkState() {
            chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);

            segmentMetadata.checkInvariants();

            Preconditions.checkArgument(offset < segmentMetadata.getLength(), "Offset %s is beyond the last offset %s of the segment %s.",
                    offset, segmentMetadata.getLength(), streamSegmentName);

            if (offset < segmentMetadata.getStartOffset()) {
                throw new CompletionException(new StreamSegmentTruncatedException(streamSegmentName, segmentMetadata.getStartOffset(), offset));
            }
        }

        private void checkPreconditions() {
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

    private static class WriteOperation implements Callable<CompletableFuture<Void>> {
        private long traceId;
        private Timer timer;
        private SegmentHandle handle;
        private long offset;
        private InputStream data;
        private int length;
        private ChunkedSegmentStorage chunkedSegmentStorage;

        private ArrayList<SystemJournal.SystemJournalRecord> systemLogRecords = new ArrayList<>();
        private List<ChunkNameOffsetPair> newReadIndexEntries = new ArrayList<>();
        private int chunksAddedCount = 0;
        private boolean isCommited = false;
        private String streamSegmentName;
        private SegmentMetadata segmentMetadata;

        private boolean isSystemSegment;

        // Check if this is a first write after ownership changed.
        private boolean isFirstWriteAfterFailover;

        private ChunkMetadata lastChunkMetadata = null;
        private ChunkHandle chunkHandle = null;
        private int bytesRemaining;
        private long currentOffset;

        private boolean didSegmentLayoutChange = false;

        WriteOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset, InputStream data, int length) {
            this.handle = handle;
            this.offset = offset;
            this.data = data;
            this.length = length;
            this.chunkedSegmentStorage = chunkedSegmentStorage;
        }

        public CompletableFuture<Void> call() {
            traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
            timer = new Timer();

            // Validate preconditions.
            checkPreconditions();

            streamSegmentName = handle.getSegmentName();
            return tryWith(chunkedSegmentStorage.metadataStore.beginTransaction(handle.getSegmentName()),
                    txn -> {
                        didSegmentLayoutChange = false;

                        // Retrieve metadata.
                        return txn.get(streamSegmentName)
                                .thenComposeAsync(storageMetadata -> {
                                    segmentMetadata = (SegmentMetadata) storageMetadata;
                                    // Validate preconditions.
                                    checkState();

                                    isSystemSegment = chunkedSegmentStorage.isStorageSystemSegment(segmentMetadata);

                                    // Check if this is a first write after ownership changed.
                                    isFirstWriteAfterFailover = segmentMetadata.isOwnershipChanged();

                                    lastChunkMetadata = null;
                                    chunkHandle = null;
                                    bytesRemaining = length;
                                    currentOffset = offset;

                                    // Get the last chunk segmentMetadata for the segment.

                                    return getLastChunk(txn)
                                            .thenComposeAsync(v ->
                                                            writeData(txn)
                                                                    .thenComposeAsync(vv ->
                                                                                    commit(txn)
                                                                                            .thenApplyAsync(vvvv ->
                                                                                                    postCommit(), chunkedSegmentStorage.executor)
                                                                                            .exceptionally(this::handleException),
                                                                            chunkedSegmentStorage.executor)
                                                                    .whenCompleteAsync((value, e) -> collectGarbage(), chunkedSegmentStorage.executor)
                                                                    .thenApplyAsync(vvv -> {
                                                                        logEnd();
                                                                        return null;
                                                                    }, chunkedSegmentStorage.executor),
                                                    chunkedSegmentStorage.executor);
                                }, chunkedSegmentStorage.executor);
                    }, chunkedSegmentStorage.executor);
        }

        private Object handleException(Throwable e) {
            val ex = Exceptions.unwrap(e);
            if (ex instanceof StorageMetadataWritesFencedOutException) {
                throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
            }
            throw new CompletionException(ex);
        }

        private Object postCommit() {
            // Post commit actions.
            // Update the read index.
            chunkedSegmentStorage.readIndexCache.addIndexEntries(streamSegmentName, newReadIndexEntries);
            return null;
        }

        private CompletableFuture<Void> getLastChunk(MetadataTransaction txn) {
            CompletableFuture<Void> f = CompletableFuture.completedFuture(null);
            if (null != segmentMetadata.getLastChunk()) {
                f = txn.get(segmentMetadata.getLastChunk())
                        .thenApplyAsync(storageMetadata1 -> {
                            lastChunkMetadata = (ChunkMetadata) storageMetadata1;
                            return null;
                        }, chunkedSegmentStorage.executor);
            }
            return f;
        }

        private void logEnd() {
            Duration elapsed = timer.getElapsed();
            log.debug("{} write - segment={}, offset={}, length={}, latency={}.", chunkedSegmentStorage.logPrefix, handle.getSegmentName(), offset, length, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "write", traceId, handle, offset);
        }

        private void collectGarbage() {
            if (!isCommited && chunksAddedCount > 0) {
                // Collect garbage.
                chunkedSegmentStorage.collectGarbage(newReadIndexEntries.stream().map(ChunkNameOffsetPair::getChunkName).collect(Collectors.toList()));
            }
        }

        private CompletableFuture<Void> commit(MetadataTransaction txn) {
            // commit all system log records if required.
            if (isSystemSegment && chunksAddedCount > 0) {
                // commit all system log records.
                Preconditions.checkState(chunksAddedCount == systemLogRecords.size());
                txn.setExternalCommitStep(() -> {
                    chunkedSegmentStorage.systemJournal.commitRecords(systemLogRecords);
                    return null;
                });
            }

            // if layout did not change then commit with lazyWrite.
            return txn.commit(!didSegmentLayoutChange &&  chunkedSegmentStorage.config.isLazyCommitEnabled())
                    .thenApplyAsync(v -> {
                        isCommited = true;
                        return null;
                    }, chunkedSegmentStorage.executor);

        }

        private CompletableFuture<Void> writeData(MetadataTransaction txn) {
            return Futures.loop(
                    () -> bytesRemaining > 0,
                    () -> {
                        // Check if new chunk needs to be added.
                        // This could be either because there are no existing chunks or last chunk has reached max rolling length.
                        return openChunkToWrite(txn)
                                .thenComposeAsync(v -> {
                                    // Calculate the data that needs to be written.
                                    long offsetToWriteAt = currentOffset - segmentMetadata.getLastChunkStartOffset();
                                    int writeSize = (int) Math.min(bytesRemaining, segmentMetadata.getMaxRollinglength() - offsetToWriteAt);

                                    // Write data to last chunk.
                                    return writeToChunk(txn,
                                            segmentMetadata,
                                            data,
                                            chunkHandle,
                                            lastChunkMetadata,
                                            offsetToWriteAt,
                                            writeSize)
                                            .thenApplyAsync(bytesWritten -> {
                                                // Update the counts
                                                bytesRemaining -= bytesWritten;
                                                currentOffset += bytesWritten;
                                                return null;
                                            }, chunkedSegmentStorage.executor);
                                }, chunkedSegmentStorage.executor);
                    }, chunkedSegmentStorage.executor)
                    .thenApplyAsync(v -> {
                        // Check invariants.
                        segmentMetadata.checkInvariants();
                        return null;
                    }, chunkedSegmentStorage.executor);
        }

        private CompletableFuture<Void> openChunkToWrite(MetadataTransaction txn) {
            if (null == lastChunkMetadata
                    || (lastChunkMetadata.getLength() >= segmentMetadata.getMaxRollinglength())
                    || isFirstWriteAfterFailover
                    || !chunkedSegmentStorage.shouldAppend()) {
                return addNewChunk(txn);

            } else {
                // No new chunk needed just write data to existing chunk.
                return chunkedSegmentStorage.chunkStorage.openWrite(lastChunkMetadata.getName())
                        .thenApplyAsync(h -> {
                            chunkHandle = h;
                            return null;
                        }, chunkedSegmentStorage.executor);
            }
        }

        private CompletableFuture<Void> addNewChunk(MetadataTransaction txn) {
            // Create new chunk
            String newChunkName = getNewChunkName(streamSegmentName,
                    segmentMetadata.getLength());
            return chunkedSegmentStorage.chunkStorage.create(newChunkName)
                    .thenApplyAsync(h -> {
                        chunkHandle = h;
                        String previousLastChunkName = lastChunkMetadata == null ? null : lastChunkMetadata.getName();

                        // update first and last chunks.
                        lastChunkMetadata = updateMetadataForChunkAddition(txn,
                                segmentMetadata,
                                newChunkName,
                                isFirstWriteAfterFailover,
                                lastChunkMetadata);

                        // Record the creation of new chunk.
                        if (isSystemSegment) {
                            addSystemLogRecord(systemLogRecords,
                                    streamSegmentName,
                                    segmentMetadata.getLength(),
                                    previousLastChunkName,
                                    newChunkName);
                            txn.markPinned(lastChunkMetadata);
                        }
                        // Update read index.
                        newReadIndexEntries.add(new ChunkNameOffsetPair(segmentMetadata.getLength(), newChunkName));

                        isFirstWriteAfterFailover = false;
                        didSegmentLayoutChange = true;
                        chunksAddedCount++;

                        log.debug("{} write - New chunk added - segment={}, chunk={}, offset={}.",
                                chunkedSegmentStorage.logPrefix, streamSegmentName, newChunkName, segmentMetadata.getLength());
                        return null;
                    }, chunkedSegmentStorage.executor);
        }

        private void checkState() {
            chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);
            segmentMetadata.checkInvariants();
            chunkedSegmentStorage.checkNotSealed(streamSegmentName, segmentMetadata);
            chunkedSegmentStorage.checkOwnership(streamSegmentName, segmentMetadata);

            // Validate that offset is correct.
            if ((segmentMetadata.getLength()) != offset) {
                throw new CompletionException(new BadOffsetException(streamSegmentName, segmentMetadata.getLength(), offset));
            }
        }

        private void checkPreconditions() {
            Preconditions.checkArgument(null != handle, "handle");
            Preconditions.checkArgument(null != data, "data");
            Preconditions.checkArgument(null != handle.getSegmentName(), "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");
            Preconditions.checkArgument(offset >= 0, "offset");
            Preconditions.checkArgument(length >= 0, "length");
        }

        private String getNewChunkName(String segmentName, long offset) {
            return NameUtils.getSegmentChunkName(segmentName, chunkedSegmentStorage.epoch, offset);
        }

        /**
         * Updates the segment metadata for the newly added chunk.
         */
        private ChunkMetadata updateMetadataForChunkAddition(MetadataTransaction txn,
                                                             SegmentMetadata segmentMetadata,
                                                             String newChunkName,
                                                             boolean isFirstWriteAfterFailover,
                                                             ChunkMetadata lastChunkMetadata) {
            ChunkMetadata newChunkMetadata = ChunkMetadata.builder()
                    .name(newChunkName)
                    .build();
            segmentMetadata.setLastChunk(newChunkName);
            if (lastChunkMetadata == null) {
                segmentMetadata.setFirstChunk(newChunkName);
            } else {
                lastChunkMetadata.setNextChunk(newChunkName);
                txn.update(lastChunkMetadata);
            }
            segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());

            // Reset ownershipChanged flag after first write is done.
            if (isFirstWriteAfterFailover) {
                segmentMetadata.setOwnerEpoch(chunkedSegmentStorage.epoch);
                segmentMetadata.setOwnershipChanged(false);
                log.debug("{} write - First write after failover - segment={}.", chunkedSegmentStorage.logPrefix, segmentMetadata.getName());
            }
            segmentMetadata.incrementChunkCount();

            // Update the transaction.
            txn.update(newChunkMetadata);
            txn.update(segmentMetadata);
            return newChunkMetadata;
        }

        /**
         * Adds a system log.
         *
         * @param systemLogRecords  List of system records.
         * @param streamSegmentName Name of the segment.
         * @param offset            Offset at which new chunk was added.
         * @param oldChunkName      Name of the previous last chunk.
         * @param newChunkName      Name of the new last chunk.
         */
        private void addSystemLogRecord(ArrayList<SystemJournal.SystemJournalRecord> systemLogRecords, String streamSegmentName, long offset, String oldChunkName, String newChunkName) {
            systemLogRecords.add(
                    SystemJournal.ChunkAddedRecord.builder()
                            .segmentName(streamSegmentName)
                            .offset(offset)
                            .oldChunkName(oldChunkName)
                            .newChunkName(newChunkName)
                            .build());
        }

        /**
         * Write to chunk.
         */
        private CompletableFuture<Integer> writeToChunk(MetadataTransaction txn,
                                                        SegmentMetadata segmentMetadata,
                                                        InputStream data,
                                                        ChunkHandle chunkHandle,
                                                        ChunkMetadata chunkWrittenMetadata,
                                                        long offsetToWriteAt,
                                                        int bytesCount) {
            Preconditions.checkState(0 != bytesCount, "Attempt to write zero bytes");
            // Finally write the data.
            BoundedInputStream bis = new BoundedInputStream(data, bytesCount);
            return chunkedSegmentStorage.chunkStorage.write(chunkHandle, offsetToWriteAt, bytesCount, bis)
                    .thenApplyAsync(bytesWritten -> {
                        // Update the metadata for segment and chunk.
                        Preconditions.checkState(bytesWritten >= 0);
                        segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
                        chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
                        txn.update(chunkWrittenMetadata);
                        txn.update(segmentMetadata);
                        return bytesWritten;
                    }, chunkedSegmentStorage.executor)
                    .exceptionally(e -> {
                        val ex = Exceptions.unwrap(e);
                        if (ex instanceof InvalidOffsetException) {
                            throw new CompletionException(new BadOffsetException(segmentMetadata.getName(),
                                    ((InvalidOffsetException) ex).getExpectedOffset(),
                                    ((InvalidOffsetException) ex).getGivenOffset()));
                        }
                        if (ex instanceof ChunkStorageException) {
                            throw new CompletionException(ex);
                        }

                        throw new CompletionException(ex);
                    });
        }
    }

    /**
     * Defragments the list of chunks for a given segment.
     * It finds eligible consecutive chunks that can be merged together.
     * The sublist such elgible chunks is replaced with single new chunk record corresponding to new large chunk.
     * Conceptually this is like deleting nodes from middle of the list of chunks.
     *
     * <Ul>
     * <li> In the absence of defragmentation, the number of chunks for individual segments keeps on increasing.
     * When we have too many small chunks (say because many transactions with little data on some segments), the segment
     * is fragmented - this may impact both the read throughput and the performance of the metadata store.
     * This problem is further intensified when we have stores that do not support append semantics (e.g., stock S3) and
     * each write becomes a separate chunk.
     * </li>
     * <li>
     * If the underlying storage provides some facility to stitch together smaller chunk into larger chunks, then we do
     * actually want to exploit that, specially when the underlying implementation is only a metadata operation. We want
     * to leverage multi-part uploads in object stores that support it (e.g., AWS S3, Dell EMC ECS) as they are typically
     * only metadata operations, reducing the overall cost of the merging them together. HDFS also supports merges,
     * whereas NFS has no concept of merging natively.
     *
     * As chunks become larger, append writes (read source completely and append it back at the end of target)
     * become inefficient. Consequently, a native option for merging is desirable. We use such native merge capability
     * when available, and if not available, then we use appends.
     * </li>
     * <li>
     * Ideally we want the defrag to be run in the background periodically and not on the write/concat path.
     * We can then fine tune that background task to run optimally with low overhead.
     * We might be able to give more knobs to tune its parameters (Eg. threshold on number of chunks).
     * </li>
     * <li>
     * <li>
     * Defrag operation will respect max rolling size and will not create chunks greater than that size.
     * </li>
     * </ul>
     *
     * What controls whether we invoke concat or simulate through appends?
     * There are a few different capabilities that ChunkStorage needs to provide.
     * <ul>
     * <li>Does ChunkStorage support appending to existing chunks? For vanilla S3 compatible this would return false.
     * This is indicated by supportsAppend.</li>
     * <li>Does ChunkStorage support for concatenating chunks ? This is indicated by supportsConcat.
     * If this is true then concat operation will be invoked otherwise chunks will be appended.</li>
     * <li>There are some obvious constraints - For ChunkStorage support any concat functionality it must support either
     * append or concat.</li>
     * <li>Also when ChunkStorage supports both concat and append, ChunkedSegmentStorage will invoke appropriate method
     * depending on size of target and source chunks. (Eg. ECS)</li>
     * </ul>
     *
     * <li>
     * What controls defrag?
     * There are two additional parameters that control when concat
     * <li>minSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered a small object.
     * For small source objects, append is used instead of using concat. (For really small txn it is rather efficient to use append than MPU).</li>
     * <li>maxSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered for concat. (Eg S3 might have max limit on chunk size).</li>
     * In short there is a size beyond which using append is not advisable. Conversely there is a size below which concat is not efficient.(minSizeLimitForConcat )
     * Then there is limit which concating does not make sense maxSizeLimitForConcat
     * </li>
     * <li>
     * What is the defrag algorithm
     * <pre>
     * While(segment.hasConcatableChunks()){
     *     Set<List<Chunk>> s = FindConsecutiveConcatableChunks();
     *     For (List<chunk> list : s){
     *        ConcatChunks (list);
     *     }
     * }
     * </pre>
     * </li>
     * </ul>
     */
    private static class DefragmentOperation implements Callable<CompletableFuture<Void>> {
        final private MetadataTransaction txn;
        final private SegmentMetadata segmentMetadata;
        final private String startChunkName;
        final private String lastChunkName;
        final private ArrayList<String> chunksToDelete;
        final private ChunkedSegmentStorage chunkedSegmentStorage;

        private ArrayList<ChunkInfo> chunksToConcat = new ArrayList<>();

        private ChunkMetadata target;
        private String targetChunkName;
        private boolean useAppend;
        private long targetSizeAfterConcat;
        private String nextChunkName;
        private ChunkMetadata next = null;

        private long writeAtOffset;
        private int readAtOffset = 0;
        private int bytesToRead;
        private int currentArgIndex;

        DefragmentOperation(ChunkedSegmentStorage chunkedSegmentStorage, MetadataTransaction txn, SegmentMetadata segmentMetadata, String startChunkName, String lastChunkName, ArrayList<String> chunksToDelete) {
            this.txn = txn;
            this.segmentMetadata = segmentMetadata;
            this.startChunkName = startChunkName;
            this.lastChunkName = lastChunkName;
            this.chunksToDelete = chunksToDelete;
            this.chunkedSegmentStorage = chunkedSegmentStorage;
        }

        public CompletableFuture<Void> call() {
            // The algorithm is actually very simple.
            // It tries to concat all small chunks using appends first.
            // Then it tries to concat remaining chunks using concat if available.
            // To implement it using single loop we toggle between concat with append and concat modes. (Instead of two passes.)
            useAppend = true;
            targetChunkName = startChunkName;

            // Iterate through chunk list
            return Futures.loop(
                    () -> null != targetChunkName && !targetChunkName.equals(lastChunkName),
                    () -> gatherChunks()
                            .thenComposeAsync(v -> {
                                // Note - After above while loop is exited nextChunkName points to chunk next to last one to be concat.
                                // Which means target should now point to it as next after concat is complete.

                                // If there are chunks that can be appended together then concat them.
                                CompletableFuture<Void> f;
                                if (chunksToConcat.size() > 1) {
                                    // Concat
                                    f = concatChunks();
                                } else {
                                    f = CompletableFuture.completedFuture(null);
                                }
                                return f.thenApplyAsync(vv -> {
                                    // Move on to next place in list where we can concat if we are done with append based concats.
                                    if (!useAppend) {
                                        targetChunkName = nextChunkName;
                                    }
                                    // Toggle
                                    useAppend = !useAppend;
                                    return null;
                                }, chunkedSegmentStorage.executor);
                            }, chunkedSegmentStorage.executor),
                    chunkedSegmentStorage.executor)
                    .thenApplyAsync(vv -> {
                        // Make sure no invariants are broken.
                        segmentMetadata.checkInvariants();
                        return null;
                    }, chunkedSegmentStorage.executor);
        }

        private CompletableFuture<Void> concatChunks() {
            ConcatArgument[] concatArgs = new ConcatArgument[chunksToConcat.size()];
            for (int i = 0; i < chunksToConcat.size(); i++) {
                concatArgs[i] = ConcatArgument.fromChunkInfo(chunksToConcat.get(i));
            }
            CompletableFuture<Integer> f;
            if (!useAppend && chunkedSegmentStorage.chunkStorage.supportsConcat()) {
                f = chunkedSegmentStorage.chunkStorage.concat(concatArgs);
            } else {
                f = concatUsingAppend(concatArgs);
            }

            return f.thenApplyAsync(v -> {
                // Delete chunks.
                for (int i = 1; i < chunksToConcat.size(); i++) {
                    chunksToDelete.add(chunksToConcat.get(i).getName());
                }

                // Set the pointers
                target.setLength(targetSizeAfterConcat);
                target.setNextChunk(nextChunkName);

                // If target is the last chunk after this then update metadata accordingly
                if (null == nextChunkName) {
                    segmentMetadata.setLastChunk(target.getName());
                    segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength() - target.getLength());
                }

                // Update metadata for affected chunks.
                for (int i = 1; i < concatArgs.length; i++) {
                    txn.delete(concatArgs[i].getName());
                    segmentMetadata.decrementChunkCount();
                }
                txn.update(target);
                txn.update(segmentMetadata);
                return null;
            }, chunkedSegmentStorage.executor);

        }

        private CompletableFuture<Void> gatherChunks() {
            return txn.get(targetChunkName)
                    .thenComposeAsync(storageMetadata -> {
                        target = (ChunkMetadata) storageMetadata;
                        chunksToConcat = new ArrayList<>();
                        targetSizeAfterConcat = target.getLength();

                        // Add target to the list of chunks
                        chunksToConcat.add(new ChunkInfo(targetSizeAfterConcat, targetChunkName));

                        nextChunkName = target.getNextChunk();
                        return txn.get(nextChunkName)
                                .thenComposeAsync(storageMetadata1 -> {

                                    next = (ChunkMetadata) storageMetadata1;
                                    // Gather list of chunks that can be appended together.
                                    return Futures.loop(
                                            () ->
                                                    null != nextChunkName
                                                            && !(useAppend && chunkedSegmentStorage.config.getMinSizeLimitForConcat() < next.getLength())
                                                            && !(targetSizeAfterConcat + next.getLength() > segmentMetadata.getMaxRollinglength() || next.getLength() > chunkedSegmentStorage.config.getMaxSizeLimitForConcat()),
                                            () -> txn.get(nextChunkName)
                                                    .thenApplyAsync(storageMetadata2 -> {
                                                        next = (ChunkMetadata) storageMetadata2;
                                                        chunksToConcat.add(new ChunkInfo(next.getLength(), nextChunkName));
                                                        targetSizeAfterConcat += next.getLength();

                                                        nextChunkName = next.getNextChunk();
                                                        return null;
                                                    }, chunkedSegmentStorage.executor),
                                            chunkedSegmentStorage.executor);

                                }, chunkedSegmentStorage.executor);
                    }, chunkedSegmentStorage.executor);
        }

        private CompletableFuture<Integer> concatUsingAppend(ConcatArgument[] concatArgs) {
            writeAtOffset = concatArgs[0].getLength();
            val writeHandle = ChunkHandle.writeHandle(concatArgs[0].getName());
            currentArgIndex = 1;
            return Futures.loop(() -> currentArgIndex < concatArgs.length,
                    () -> {
                        readAtOffset = 0;
                        val arg = concatArgs[currentArgIndex];
                        bytesToRead = Math.toIntExact(arg.getLength());

                        return copyBytes(writeHandle, arg)
                                .thenApplyAsync(v -> {
                                    currentArgIndex++;
                                    return null;
                                }, chunkedSegmentStorage.executor);
                    },
                    chunkedSegmentStorage.executor)
                    .thenApplyAsync(v -> 0, chunkedSegmentStorage.executor);
        }

        private CompletableFuture<Void> copyBytes(ChunkHandle writeHandle, ConcatArgument arg) {
            return Futures.loop(
                    () -> bytesToRead > 0,
                    () -> {
                        byte[] buffer = new byte[Math.min(chunkedSegmentStorage.config.getMaxBufferSizeForChunkDataTransfer(), bytesToRead)];
                        return chunkedSegmentStorage.chunkStorage.read(ChunkHandle.readHandle(arg.getName()), readAtOffset, buffer.length, buffer, 0)
                                .thenComposeAsync(size -> {
                                    bytesToRead -= size;
                                    readAtOffset += size;
                                    return chunkedSegmentStorage.chunkStorage.write(writeHandle, writeAtOffset, size, new ByteArrayInputStream(buffer, 0, size))
                                            .thenApplyAsync(written -> {
                                                writeAtOffset += written;
                                                return null;
                                            }, chunkedSegmentStorage.executor);
                                }, chunkedSegmentStorage.executor);
                    },
                    chunkedSegmentStorage.executor
            );
        }
    }

    private static class ConcatOperation implements Callable<CompletableFuture<Void>> {
        final private long traceId;
        final private SegmentHandle targetHandle;
        final private long offset;
        final private String sourceSegment;
        final private ChunkedSegmentStorage chunkedSegmentStorage;
        final private ArrayList<String> chunksToDelete = new ArrayList<>();

        private Timer timer;
        private String targetSegmentName;
        private SegmentMetadata targetSegmentMetadata;
        private SegmentMetadata sourceSegmentMetadata;
        private ChunkMetadata targetLastChunk;
        private ChunkMetadata sourceFirstChunk;

        ConcatOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle targetHandle, long offset, String sourceSegment) {
            this.targetHandle = targetHandle;
            this.offset = offset;
            this.sourceSegment = sourceSegment;
            this.chunkedSegmentStorage = chunkedSegmentStorage;
            traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle, offset, sourceSegment);
        }

        public CompletableFuture<Void> call() {
            timer = new Timer();

            checkPreconditions();
            targetSegmentName = targetHandle.getSegmentName();

            return tryWith(chunkedSegmentStorage.metadataStore.beginTransaction(targetHandle.getSegmentName(), sourceSegment),
                    txn -> txn.get(targetSegmentName)
                            .thenComposeAsync(storageMetadata1 -> {
                                targetSegmentMetadata = (SegmentMetadata) storageMetadata1;
                                return txn.get(sourceSegment)
                                        .thenComposeAsync(storageMetadata2 -> {
                                            sourceSegmentMetadata = (SegmentMetadata) storageMetadata2;
                                            return performConcat(txn);
                                        }, chunkedSegmentStorage.executor);
                            }, chunkedSegmentStorage.executor), chunkedSegmentStorage.executor);
        }

        private CompletionStage<Void> performConcat(MetadataTransaction txn) {
            // Validate preconditions.
            checkState();

            // Update list of chunks by appending sources list of chunks.
            return updateMetadata(txn).thenComposeAsync(v -> {
                // Finally defrag immediately.
                CompletableFuture<Void> f = CompletableFuture.completedFuture(null);
                if (shouldDefrag() && null != targetLastChunk) {
                    f = chunkedSegmentStorage.defrag(txn, targetSegmentMetadata, targetLastChunk.getName(), null, chunksToDelete);
                }
                return f.thenComposeAsync(v2 -> {
                    targetSegmentMetadata.checkInvariants();

                    // Finally commit transaction.
                    return txn.commit()
                            .exceptionally(this::handleException)
                            .thenComposeAsync(v3 -> postCommit(), chunkedSegmentStorage.executor);
                }, chunkedSegmentStorage.executor);
            }, chunkedSegmentStorage.executor);
        }

        private Void handleException(Throwable e) {
            val ex = Exceptions.unwrap(e);
            if (ex instanceof StorageMetadataWritesFencedOutException) {
                throw new CompletionException(new StorageNotPrimaryException(targetSegmentName, ex));
            }
            throw new CompletionException(ex);
        }

        private CompletionStage<Void> postCommit() {
            // Collect garbage.
            return chunkedSegmentStorage.collectGarbage(chunksToDelete)
                    .thenApplyAsync(v4 -> {
                        // Update the read index.
                        chunkedSegmentStorage.readIndexCache.remove(sourceSegment);
                        logEnd();
                        return null;
                    }, chunkedSegmentStorage.executor);
        }

        private void logEnd() {
            Duration elapsed = timer.getElapsed();
            log.debug("{} concat - target={}, source={}, offset={}, latency={}.", chunkedSegmentStorage.logPrefix, targetHandle.getSegmentName(), sourceSegment, offset, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "concat", traceId, targetHandle, offset, sourceSegment);
        }

        private CompletableFuture<Void> updateMetadata(MetadataTransaction txn) {
            return txn.get(targetSegmentMetadata.getLastChunk())
                    .thenComposeAsync(storageMetadata1 -> {
                        targetLastChunk = (ChunkMetadata) storageMetadata1;
                        return txn.get(sourceSegmentMetadata.getFirstChunk())
                                .thenApplyAsync(storageMetadata2 -> {
                                    sourceFirstChunk = (ChunkMetadata) storageMetadata2;

                                    if (targetLastChunk != null) {
                                        targetLastChunk.setNextChunk(sourceFirstChunk.getName());
                                        txn.update(targetLastChunk);
                                    } else {
                                        if (sourceFirstChunk != null) {
                                            targetSegmentMetadata.setFirstChunk(sourceFirstChunk.getName());
                                            txn.update(sourceFirstChunk);
                                        }
                                    }

                                    // Update segments's last chunk to point to the sources last segment.
                                    targetSegmentMetadata.setLastChunk(sourceSegmentMetadata.getLastChunk());

                                    // Update the length of segment.
                                    targetSegmentMetadata.setLastChunkStartOffset(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLastChunkStartOffset());
                                    targetSegmentMetadata.setLength(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLength() - sourceSegmentMetadata.getStartOffset());

                                    targetSegmentMetadata.setChunkCount(targetSegmentMetadata.getChunkCount() + sourceSegmentMetadata.getChunkCount());

                                    txn.update(targetSegmentMetadata);
                                    txn.delete(sourceSegment);
                                    return null;
                                }, chunkedSegmentStorage.executor);
                    }, chunkedSegmentStorage.executor);
        }

        private void checkState() {
            chunkedSegmentStorage.checkSegmentExists(targetSegmentName, targetSegmentMetadata);
            targetSegmentMetadata.checkInvariants();
            chunkedSegmentStorage.checkNotSealed(targetSegmentName, targetSegmentMetadata);

            chunkedSegmentStorage.checkSegmentExists(sourceSegment, sourceSegmentMetadata);
            sourceSegmentMetadata.checkInvariants();

            // This is a critical assumption at this point which should not be broken,
            Preconditions.checkState(!targetSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated.");
            Preconditions.checkState(!sourceSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated.");

            checkSealed(sourceSegmentMetadata);
            chunkedSegmentStorage.checkOwnership(targetSegmentMetadata.getName(), targetSegmentMetadata);

            if (sourceSegmentMetadata.getStartOffset() != 0) {
                throw new CompletionException(new StreamSegmentTruncatedException(sourceSegment, sourceSegmentMetadata.getLength(), 0));
            }

            if (offset != targetSegmentMetadata.getLength()) {
                throw new CompletionException(new BadOffsetException(targetHandle.getSegmentName(), targetSegmentMetadata.getLength(), offset));
            }
        }

        private void checkPreconditions() {
            Preconditions.checkArgument(null != targetHandle, "targetHandle");
            Preconditions.checkArgument(!targetHandle.isReadOnly(), "targetHandle");
            Preconditions.checkArgument(null != sourceSegment, "targetHandle");
            Preconditions.checkArgument(offset >= 0, "offset");
        }

        private void checkSealed(SegmentMetadata sourceSegmentMetadata) {
            if (!sourceSegmentMetadata.isSealed()) {
                throw new IllegalStateException("Source segment must be sealed.");
            }
        }

        private boolean shouldDefrag() {
            return (chunkedSegmentStorage.shouldAppend() || chunkedSegmentStorage.chunkStorage.supportsConcat())
                    && chunkedSegmentStorage.config.isInlineDefragEnabled();
        }
    }
}
