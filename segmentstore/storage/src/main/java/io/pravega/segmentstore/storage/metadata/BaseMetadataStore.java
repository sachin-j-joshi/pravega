/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.metadata;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Implements base metadata store that provides core functionality of metadata store by encapsulating underlying key value store.
 * Derived classes override {@link BaseMetadataStore#read(String)} and {@link BaseMetadataStore#writeAll(Collection)} to write to underlying storage.
 * The minimum requirement for underlying key-value store is to provide optimistic concurrency ( Eg. using versions numbers or etags.)
 *
 *
 * Within a segment store instance there should be only one instance that exclusively writes to the underlying key value store.
 * For distributed systems the single writer pattern must be enforced through external means. (Eg. for table segment based implementation
 * DurableLog's native fencing is used to establish ownership and single writer pattern.)
 *
 * This implementation provides following features that simplify metadata management.
 *
 * All access to and modifications to the metadata the {@link ChunkMetadataStore} must be done through a transaction.
 *
 * A transaction is created by calling {@link ChunkMetadataStore#beginTransaction(String...)}
 *
 * Changes made to metadata inside a transaction are not visible until a transaction is committed using any overload of{@link MetadataTransaction#commit()}.
 * Transaction is aborted automatically unless committed or when {@link MetadataTransaction#abort()} is called.
 * Transactions are atomic - either all changes in the transaction are committed or none at all.
 * In addition, Transactions provide snaphot isolation which means that transaction fails if any of the metadata records read during the transactions are changed outside the transaction after they were read.
 *
 * Within a transaction you can perform following actions on per record basis.
 * <ul>
 * <li>{@link MetadataTransaction#get(String)} Retrieves metadata using for given key.</li>
 * <li>{@link MetadataTransaction#create(StorageMetadata)} Creates a new record.</li>
 * <li>{@link MetadataTransaction#delete(String)} Deletes records for given key.</li>
 * <li>{@link MetadataTransaction#update(StorageMetadata)} Updates the transaction local copy of the record.
 * For each record modified inside the transaction update must be called to mark the record as dirty.</li>
 * </ul>
 * <pre>
 *  // Start a transaction.
 * try (MetadataTransaction txn = metadataStore.beginTransaction()) {
 *      // Retrieve the data from transaction
 *      SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
 *
 *      // Modify retrieved record
 *      // seal if it is not already sealed.
 *      segmentMetadata.setSealed(true);
 *
 *      // put it back transaction
 *      txn.update(segmentMetadata);
 *
 *      // Commit
 *      txn.commit();
 *  } catch (StorageMetadataException ex) {
 *      // Handle Exceptions
 *  }
 *  </pre>
 *
 * Underlying implementation might buffer frequently or recently updated metadata keys to optimize read/write performance.
 * To further optimize it may provide "lazy committing" of changes where there is application specific way to recover from failures.(Eg. when only length of chunk is changed.)
 * In this case {@link MetadataTransaction#commit(boolean)} can be called.Note that otherwise for each commit the data is written to underlying key-value store.
 *
 * There are two special methods provided to handle metadata about data segments for the underlying key-value store. They are useful in avoiding circular references.
 * <ul>
 * <li>A record marked as pinned by calling {@link MetadataTransaction#markPinned(StorageMetadata)} is never written to underlying storage.</li>
 * <li>In addition transaction can be committed using {@link MetadataTransaction#commit(boolean, boolean)} to skip validation step that reads any recently evicted changes from underlying storage.</li>
 * </ul>
 */
@Slf4j
@Beta
abstract public class BaseMetadataStore implements ChunkMetadataStore {
    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    private static final int MAX_ENTRIES_IN_TXN_BUFFER = 5000;

    /**
     * Maximum wait time for acquiring individual locks.
     */
    private static final Duration LOCK_WAIT_TIME = Duration.ofSeconds(1);

    /**
     * Indicates whether this instance is fenced or not.
     */
    private final AtomicBoolean fenced;

    /**
     * Monotonically increasing number. Keeps track of versions independent of external persistence or transaction mechanism.
     */
    private final AtomicLong version;

    /**
     * Buffer for reading and writing transaction data entries to underlying KV store.
     * This allows lazy storing and avoiding unnecessary load for recently/frequently updated key value pairs.
     */
    private final ConcurrentHashMap<String, TransactionData> bufferedTxnData;

    /**
     * {@link MultiKeyReaderWriterScheduler} instance.
     */
    private final MultiKeyReaderWriterScheduler scheduler = new MultiKeyReaderWriterScheduler();

    /**
     * Storage executor object.
     */
    @Getter(AccessLevel.PROTECTED)
    private final Executor executor;

    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    @Getter
    @Setter
    int maxEntriesInTxnBuffer = MAX_ENTRIES_IN_TXN_BUFFER;

    /**
     * Constructs a BaseMetadataStore object.
     *
     * @param executor Executor to use for async operations.
     */
    public BaseMetadataStore(Executor executor) {
        version = new AtomicLong(System.currentTimeMillis()); // Start with unique number.
        fenced = new AtomicBoolean(false);
        bufferedTxnData = new ConcurrentHashMap<>(); // Don't think we need anything fancy here. But we'll measure and see.
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    /**
     * Begins a new transaction.
     *
     * @param keysToLock Array of keys to lock for this transaction.
     * @return Returns a new instance of MetadataTransaction.
     * throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public MetadataTransaction beginTransaction(String... keysToLock) {
        // Each transaction gets a unique number which is monotonically increasing.
        return new MetadataTransaction(this, version.incrementAndGet(), keysToLock);
    }

    /**
     * Commits given transaction.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public CompletableFuture<Void> commit(MetadataTransaction txn, boolean lazyWrite) {
        return commit(txn, lazyWrite, false);
    }

    /**
     * Commits given transaction.
     *
     * @param txn transaction to commit.
     * throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public CompletableFuture<Void> commit(MetadataTransaction txn) {
        return commit(txn, false, false);
    }

    /**
     * Commits given transaction.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public CompletableFuture<Void> commit(MetadataTransaction txn, boolean lazyWrite, boolean skipStoreCheck) {
        Preconditions.checkArgument(null != txn);
        final Map<String, TransactionData> txnData = txn.getData();

        final ArrayList<String> modifiedKeys = new ArrayList<>();
        final ArrayList<TransactionData> modifiedValues = new ArrayList<>();

        return CompletableFuture.runAsync(() -> {
            if (fenced.get()) {
                throw new CompletionException(new StorageMetadataWritesFencedOutException("Transaction writer is fenced off."));
            }
        }, executor).thenComposeAsync(v -> {
            // Step 1 : If bufferedTxnData data was flushed, then read it back from external source and re-insert in bufferedTxnData buffer.
            // This step is kind of thread safe
            ArrayList<CompletableFuture<TransactionData>> loadFutures = new ArrayList<>();
            for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
                String key = entry.getKey();
                if (skipStoreCheck || entry.getValue().isPinned()) {
                    log.trace("Skipping loading key from the store key = {}", key);
                } else {
                    // This check is safe to be outside the lock
                    if (!bufferedTxnData.containsKey(key)) {
                        loadFutures.add(loadFromStore(key));
                    }
                }
            }
            return CompletableFuture.allOf(loadFutures.toArray(new CompletableFuture[loadFutures.size()]));
        }, executor).thenComposeAsync(v -> {
            // Step 2 : Check whether transaction is safe to commit.
            // This check needs to be atomic, with absolutely no possibility of re-entry
            val writeLock = scheduler.getWriteLock(txn.getKeysToLock());
            return writeLock.lock()
                    .thenComposeAsync(v1 -> {
                        // If some other transaction beat us then use that value.
                        return performCommit(txn, lazyWrite, txnData, modifiedKeys, modifiedValues);
                    }, executor).whenCompleteAsync((v2, ex) -> {
                        writeLock.unlock();
                    }, executor);
        }, executor).thenRunAsync(() -> {
            //  Step 5 : evict if required.
            if (bufferedTxnData.size() > maxEntriesInTxnBuffer) {
                bufferedTxnData.entrySet().removeIf(entry -> entry.getValue().isPersisted() && !entry.getValue().isPinned());
            }

            //  Step 6: finally clear
            txn.setCommited();
            txnData.clear();
        }, executor);
    }

    private CompletableFuture<Void> performCommit(MetadataTransaction txn, boolean lazyWrite, Map<String, TransactionData> txnData, ArrayList<String> modifiedKeys, ArrayList<TransactionData> modifiedValues) {
        return CompletableFuture.runAsync(() -> {
            for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
                String key = entry.getKey();
                val transactionData = entry.getValue();
                Preconditions.checkState(null != transactionData.getKey());

                // See if this entry was modified in this transaction.
                if (transactionData.getVersion() == txn.getVersion()) {
                    modifiedKeys.add(key);
                    transactionData.setPersisted(false);
                    modifiedValues.add(transactionData);
                }
                // make sure none of the keys used in this transaction have changed.
                TransactionData dataFromBuffer = bufferedTxnData.get(key);
                if (null != dataFromBuffer) {
                    if (dataFromBuffer.getVersion() > transactionData.getVersion()) {
                        throw new CompletionException(new StorageMetadataVersionMismatchException(
                                String.format("Transaction uses stale data. Key version changed key:%s buffer:%s transaction:%s",
                                        key, dataFromBuffer.getVersion(), txnData.get(key).getVersion())));
                    }

                    // Pin it if it is already pinned.
                    transactionData.setPinned(transactionData.isPinned() || dataFromBuffer.isPinned());

                    // Set the database object.
                    transactionData.setDbObject(dataFromBuffer.getDbObject());
                }
            }
        }, executor).thenComposeAsync(v -> {
            // Step 3: Commit externally.
            // This operation may call external storage.
            if (!lazyWrite || (bufferedTxnData.size() > maxEntriesInTxnBuffer)) {
                log.trace("Persisting all modified keys (except pinned)");
                val toWriteList = modifiedValues.stream().filter(entry -> !entry.isPinned()).collect(Collectors.toList());
                return writeAll(toWriteList).thenRunAsync(() -> {
                    log.trace("Done persisting all modified keys");

                    // Mark written keys as persisted.
                    for (val writtenData : toWriteList) {
                        writtenData.setPersisted(true);
                    }
                }, executor);
            }
            return CompletableFuture.completedFuture(null);
        }, executor).thenComposeAsync(v -> {
            return CompletableFuture.supplyAsync(() -> {
                // Execute external commit step.
                try {
                    if (null != txn.getExternalCommitStep()) {
                        txn.getExternalCommitStep().call();
                    }
                } catch (Exception e) {
                    log.error("Exception during execution of external commit step", e);
                    throw new CompletionException(new StorageMetadataException("Exception during execution of external commit step", e));
                }
                return null;
            }, executor);
        }, executor).thenRunAsync(() -> {
            // If we reach here then it means transaction is safe to commit.
            // Step 4: Insert
            long committedVersion = version.incrementAndGet();
            HashMap<String, TransactionData> toAdd = new HashMap<String, TransactionData>();
            for (String key : modifiedKeys) {
                TransactionData data = txnData.get(key);
                data.setVersion(committedVersion);
                toAdd.put(key, data);
            }
            bufferedTxnData.putAll(toAdd);
        }, executor);
    }

    /**
     * Aborts given transaction.
     *
     * @param txn transaction to abort.
     * throws StorageMetadataException If there are any errors.
     */
    public CompletableFuture abort(MetadataTransaction txn) {
        // Do nothing
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Retrieves the metadata for given key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @return Metadata for given key. Null if key was not found.
     * throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public CompletableFuture<StorageMetadata> get(MetadataTransaction txn, String key) {
        Preconditions.checkArgument(null != txn);
        if (null == key) {
            return CompletableFuture.completedFuture(null);
        }

        Map<String, TransactionData> txnData = txn.getData();

        // Record is found in transaction data itself.
        TransactionData data = txnData.get(key);
        if (null != data) {
            return CompletableFuture.completedFuture(data.getValue());
        }

        // Try to find it in buffer. Access buffer using reader lock
        val readLock = scheduler.getReadLock(new String[]{key});
        return readLock.lock()
                .thenApplyAsync(v -> bufferedTxnData.get(key), executor)
                .thenApplyAsync(dataFromBuffer -> {
                    if (dataFromBuffer != null) {
                        // Make sure it is a deep copy.
                        val retValue =  dataFromBuffer.getValue();
                        if (null != retValue) {
                            return retValue.deepCopy();
                        }
                        return retValue;
                    }
                    return null;
                }, executor).whenCompleteAsync((v, ex) -> {
                    readLock.unlock();
                }, executor).thenComposeAsync(retValue -> {
                    if (retValue != null) {
                        return CompletableFuture.completedFuture(retValue);
                    }
                    // We did not find it in the buffer either.
                    // Try to find it in store.
                    return loadFromStore(key)
                            .thenApplyAsync(fromStore -> {
                                if (fromStore != null) {
                                    return fromStore.getValue();
                                }
                                return null;
                            }, executor);
                }, executor);
    }

    /**
     * Loads value from store
     *
     * @param key Key to load
     * @return Value if found null otherwise.
     */
    private CompletableFuture<TransactionData> loadFromStore(String key) {
        log.trace("Loading key from the store key = {}", key);
        return read(key)
                .thenApplyAsync(fromDb -> {
                    Preconditions.checkState(null != fromDb);
                    log.trace("Done Loading key from the store key = {}", key);

                    TransactionData copyForBuffer = fromDb.toBuilder()
                            .key(key)
                            .build();

                    if (null != fromDb.getValue()) {
                        Preconditions.checkState(0 != fromDb.getVersion(), "Version is not initialized");
                        // Make sure it is a deep copy.
                        copyForBuffer.setValue(fromDb.getValue().deepCopy());
                    }
                    return copyForBuffer;
                }, executor).thenComposeAsync(copyForBuffer -> {
                    val writeLock = scheduler.getWriteLock(new String[]{key});
                    // Put this value in bufferedTxnData buffer.
                    return writeLock.lock()
                            .thenApplyAsync(lock -> {
                                // If some other transaction beat us then use that value.
                                TransactionData oldValue = bufferedTxnData.putIfAbsent(key, copyForBuffer);
                                TransactionData retValue = copyForBuffer;
                                if (oldValue != null) {
                                    retValue = oldValue;
                                }
                                return retValue;
                            }, executor).whenCompleteAsync((v, ex) -> {
                                writeLock.unlock();
                            }, executor);
                }, executor);
    }

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return A CompletableFuture that, when completed, will contain associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     */
    abstract protected CompletableFuture<TransactionData> read(String key);

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     */
    abstract protected CompletableFuture<Void> writeAll(Collection<TransactionData> dataList);

    /**
     * Updates existing metadata.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void update(MetadataTransaction txn, StorageMetadata metadata) {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != metadata);
        Preconditions.checkArgument(null != metadata.getKey());
        Map<String, TransactionData> txnData = txn.getData();

        String key = metadata.getKey();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }
        data.setValue(metadata);
        data.setPersisted(false);
        Preconditions.checkState(txn.getVersion() >= data.getVersion());
        data.setVersion(txn.getVersion());
    }

    /**
     * Marks given record as pinned.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void markPinned(MetadataTransaction txn, StorageMetadata metadata) {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != metadata);
        Map<String, TransactionData> txnData = txn.getData();
        String key = metadata.getKey();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }

        data.setValue(metadata);
        data.setPinned(true);
        data.setVersion(txn.getVersion());
    }

    /**
     * Creates a new metadata record.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void create(MetadataTransaction txn, StorageMetadata metadata) {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != metadata);
        Preconditions.checkArgument(null != metadata.getKey());
        Map<String, TransactionData> txnData = txn.getData();
        txnData.put(metadata.getKey(), TransactionData.builder()
                .key(metadata.getKey())
                .value(metadata)
                .version(txn.getVersion())
                .build());
    }

    /**
     * Deletes a metadata record given the key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void delete(MetadataTransaction txn, String key) {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != key);
        Map<String, TransactionData> txnData = txn.getData();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }
        data.setValue(null);
        data.setPersisted(false);
        data.setVersion(txn.getVersion());
    }

    /**
     * {@link AutoCloseable#close()} implementation.
     */
    @Override
    public void close() throws Exception {
        ArrayList<TransactionData> modifiedValues = new ArrayList<>();
        bufferedTxnData.entrySet().stream().filter(entry -> !entry.getValue().isPersisted() && !entry.getValue().isPinned()).forEach(entry -> modifiedValues.add(entry.getValue()));
        if (modifiedValues.size() > 0) {
            writeAll(modifiedValues);
        }
    }

    /**
     * Explicitly marks the store as fenced.
     * Once marked fenced no modifications to data should be allowed.
     */
    public void markFenced() {
        this.fenced.set(true);
    }

    /**
     * Retrieves the current version number.
     *
     * @return current version number.
     */
    protected long getVersion() {
        return version.get();
    }

    /**
     * Sets the current version number.
     *
     * @param version Version to set.
     */
    protected void setVersion(long version) {
        this.version.set(version);
    }

    /**
     * Stores the transaction data.
     */
    @Builder(toBuilder = true)
    @Data
    public static class TransactionData implements Serializable {

        /**
         * Serializer for {@link StorageMetadata}.
         */
        private final static StorageMetadata.StorageMetadataSerializer SERIALIZER = new StorageMetadata.StorageMetadataSerializer();
        /**
         * Version. This version number is independent of version in the store.
         * This is required to keep track of all modifications to data when it is changed while still in buffer without writing it to database.
         */
        private long version;

        /**
         * Implementation specific object to keep track of underlying db version.
         */
        private Object dbObject;

        /**
         * Whether this record is persisted or not.
         */
        private boolean persisted;

        /**
         * Whether this record is pinned to the memory.
         */
        private boolean pinned;

        /**
         * Key of the record.
         */
        private String key;

        /**
         * Value of the record.
         */
        private StorageMetadata value;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TransactionDataBuilder implements ObjectBuilder<TransactionData> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class TransactionDataSerializer
                extends VersionedSerializer.WithBuilder<TransactionData, TransactionDataBuilder> {
            @Override
            protected TransactionDataBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput input, TransactionDataBuilder b) throws IOException {
                b.version(input.readLong());
                b.key(input.readUTF());
                boolean hasValue = input.readBoolean();
                if (hasValue) {
                    b.value(SERIALIZER.deserialize(input.getBaseStream()));
                }
            }

            private void write00(TransactionData object, RevisionDataOutput output) throws IOException {
                output.writeLong(object.version);
                output.writeUTF(object.key);
                boolean hasValue = object.value != null;
                output.writeBoolean(hasValue);
                if (hasValue) {
                    SERIALIZER.serialize(output, object.value);
                }
            }
        }
    }
}
