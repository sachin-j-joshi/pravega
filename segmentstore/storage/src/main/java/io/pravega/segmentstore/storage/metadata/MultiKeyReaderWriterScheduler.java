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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class MultiKeyReaderWriterScheduler {

    HashMap<String, SchedulerData> keyToDataMap = new HashMap<>();
    static class SchedulerData {
        int count;
        CompletableFuture blockingFuture = CompletableFuture.completedFuture(null);
        ArrayList<CompletableFuture> readerFutures = new ArrayList<>();
    }

    @RequiredArgsConstructor
    static class MultiKeyReaderWriterAsyncLock {
        @Getter
        final String[] keys;
        @Getter
        final boolean isReadonly;

        final MultiKeyReaderWriterScheduler scheduler;

        CompletableFuture<Void> readyFuture;
        @Getter
        CompletableFuture<Void> doneFuture = new CompletableFuture<Void>();

        CompletableFuture<Void> lock() {
            if (isReadonly) {
                return scheduler.scheduleForRead(this);
            } else {
                return scheduler.scheduleForWrite(this);
            }
        }

        void unlock() {
            scheduler.release(this);
        }
    }

    private SchedulerData addReference(String key) {
        SchedulerData schedulerData;
        synchronized (keyToDataMap) {
            schedulerData = keyToDataMap.get(key);
            // Add if this is a new key.
            if (null == schedulerData) {
                schedulerData = new SchedulerData();
                keyToDataMap.put(key, schedulerData);
            }
            // Increment ref count.
            schedulerData.count++;
        }
        return schedulerData;
    }

    private void releaseReference(String key) {
        synchronized (keyToDataMap) {
            SchedulerData schedulerData = keyToDataMap.get(key);
            // Decrement ref count.
            schedulerData.count--;
            // clean up if required.
            if (0 == schedulerData.count) {
                keyToDataMap.remove(key);
            }
        }
    }

    MultiKeyReaderWriterAsyncLock getReadLock(String[] keys) {
        return new MultiKeyReaderWriterAsyncLock(keys, true, this);
    }

    MultiKeyReaderWriterAsyncLock getWriteLock(String[] keys) {
        return new MultiKeyReaderWriterAsyncLock(keys, false, this);
    }

    synchronized CompletableFuture<Void> scheduleForRead(MultiKeyReaderWriterAsyncLock lock) {
        CompletableFuture[] futuresToBlockOn = new CompletableFuture[lock.getKeys().length];
        for (int i = 0; i < lock.getKeys().length; i++) {
            val key = lock.getKeys()[i];
            SchedulerData schedulerData = addReference(key);

            futuresToBlockOn[i] = schedulerData.blockingFuture;
            // Add this as reader
            schedulerData.readerFutures.add(lock.doneFuture);
        }
        lock.readyFuture = CompletableFuture.allOf(futuresToBlockOn);
        return lock.readyFuture;
    }

    synchronized CompletableFuture<Void> scheduleForWrite(MultiKeyReaderWriterAsyncLock lock) {
        CompletableFuture[] futuresToBlockOn = new CompletableFuture[lock.getKeys().length];
        for (int i = 0; i < lock.getKeys().length; i++) {
            val key = lock.getKeys()[i];
            // Get existing data.
            SchedulerData schedulerData = addReference(key);

            // If there are outstanding readers then first "drain" all readers by making this write wait on them.
            if (schedulerData.readerFutures.size() > 0) {
                CompletableFuture[] readFutures = schedulerData.readerFutures.toArray(new CompletableFuture[schedulerData.readerFutures.size()]);
                schedulerData.blockingFuture = CompletableFuture.allOf(readFutures);
                // Empty readers
                schedulerData.readerFutures = new ArrayList<>();
            }

            // Add it to list of futures to block on.
            futuresToBlockOn[i] = schedulerData.blockingFuture;

            // Make completion of this lock's release as future on which all new requests on this key will be waiting/blocking.
            schedulerData.blockingFuture = lock.doneFuture;
        }
        // The code in lock is run when all pending futures are complete.
        lock.readyFuture = CompletableFuture.allOf(futuresToBlockOn);

        return lock.readyFuture;
    }

    void release(MultiKeyReaderWriterAsyncLock lock) {
        for (val key: lock.getKeys()) {
            releaseReference(key);
        }
        lock.doneFuture.complete(null);
    }
}
