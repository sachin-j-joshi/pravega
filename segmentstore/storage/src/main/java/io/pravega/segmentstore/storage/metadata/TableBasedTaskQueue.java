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
package io.pravega.segmentstore.storage.metadata;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.storage.chunklayer.AbstractTaskQueue;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.GarbageCollector;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link AbstractTaskQueue} using {@link TableBasedMetadataStore}.
 */
@Slf4j
public class TableBasedTaskQueue implements AbstractTaskQueue<GarbageCollector.TaskInfo> {
    public static final int CHECK_POINT_FREQUENCY = 100;
    @Getter
    @Setter
    public  int checkPointFrequency = CHECK_POINT_FREQUENCY;
    private final ChunkedSegmentStorage storage;
    private final Map<String, TableBasedQueueInfo> map = Collections.synchronizedMap(new HashMap<>());
    private final int containerId;
    private final GarbageCollector.TaskInfo.Serializer serializer = new GarbageCollector.TaskInfo.Serializer();
    private final ScheduledFuture processorTask;
    /**
     * Constructor.
     *
     * @param containerId container id.
     * @param storage instance of {@link ChunkedSegmentStorage}.
     */
    public TableBasedTaskQueue(int containerId, ChunkedSegmentStorage storage) {
        this.containerId = containerId;
        this.storage = Preconditions.checkNotNull(storage);
        processorTask = ((ScheduledExecutorService) storage.getExecutor()).scheduleAtFixedRate(() -> process(),
                storage.getConfig().getGarbageCollectionDelay().toMillis(),
                storage.getConfig().getGarbageCollectionSleep().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void process() {
        ArrayList<GarbageCollector.TaskInfo> batch = new ArrayList<>();
        val queueName = storage.getGarbageCollector().getTaskQueueName();
        val queueInfo = map.get(queueName);
        if (null == queueInfo) {
            return;
        }
        try {
            long currentId;
            long oldId = map.get(queueName).startTaskId.get();
            for (currentId = oldId; currentId < checkPointFrequency && currentId < map.get(queueName).endTaskId.get(); currentId++) {
                String keyName = getKeyName(queueName, currentId);
                @Cleanup
                val txn = storage.getMetadataStore().beginTransaction(false, keyName);
                val info = txn.get(keyName).get();
                if (null != info) {
                    TableBasedTaskInfo taskInfo = (TableBasedTaskInfo) info;
                    batch.add((GarbageCollector.TaskInfo) taskInfo.getTaskInfo());
                }
            }
            if (batch.size() > 0) {
                storage.getGarbageCollector().processBatch(batch).join();
                map.get(queueName).startTaskId.set(currentId);

                for (currentId = oldId; currentId < checkPointFrequency && currentId < map.get(queueName).endTaskId.get(); currentId++) {
                    String keyName = getKeyName(queueName, currentId);
                    @Cleanup
                    val txn = storage.getMetadataStore().beginTransaction(false, keyName);
                    txn.delete(keyName);
                    txn.commit().join();
                }
            }
        } catch (RuntimeException e) {
            log.error("TableBasedTaskQueue[{}]", containerId, e);
        } catch (Exception e) {
            log.error("TableBasedTaskQueue[{}]", containerId, e);
        }
    }

    private String getKeyName(String queueName, long currentId) {
        return "TQ." + containerId + "." + queueName + "-" + currentId;
    }

    /**
     * Adds a queue by the given name.
     *
     * @param queueName        Name of the queue.
     * @param ignoreProcessing Whether the processing should be ignored.
     */
    @Override
    public CompletableFuture<Void> addQueue(String queueName, Boolean ignoreProcessing) {
        @Cleanup
        MetadataTransaction txn = storage.getMetadataStore().beginTransaction(false, queueName);
        String keyName = "TQ." + containerId + "." + queueName;

        return txn.get(keyName).thenComposeAsync( storageMetadata -> {
            TableBasedQueueInfo info = (TableBasedQueueInfo) storageMetadata;
            if (null == info) {
                log.info("TableBasedTaskQueue[{}]: initializing info for queue={}", containerId, keyName);
                info = TableBasedQueueInfo.builder()
                        .name(keyName)
                        .ignoreProcessing(ignoreProcessing)
                        .startTaskId(new AtomicLong())
                        .endTaskId(new AtomicLong())
                        .lastBatch(new AtomicLong())
                        .build();
                map.put(queueName, info);
                txn.create(info);
                return txn.commit();

            } else {
                log.info("TableBasedTaskQueue[{}]: loading info for queue={} info={}", containerId, keyName, info);
                info.endTaskId.set(info.lastBatch.get() + checkPointFrequency);
                map.put(queueName, info);
                return saveQueueState(queueName, info);
            }
        });
    }

    /**
     * Adds a task to queue.
     *
     * @param queueName Name of the queue.
     * @param task      Task to add.
     */
    @Override
    public CompletableFuture<Void> addTask(String queueName, GarbageCollector.TaskInfo task) {
        val q = map.get(queueName);
        val id = q.endTaskId.incrementAndGet();
        CompletableFuture<Void> f = null;
        if (id - q.lastBatch.get() > checkPointFrequency) {
            synchronized (q) {
                if (id - q.lastBatch.get() > checkPointFrequency) {
                    f = saveQueueState(queueName, q);
                }
            }
        }

        try (val txn = storage.getMetadataStore().beginTransaction(false, task.getName())) {
            String keyName = "TQ." + containerId + "." + queueName + "-" + id;
            TableBasedTaskInfo tt = TableBasedTaskInfo.builder()
                    .id(keyName)
                    .taskInfo(task)
                    .build();
            txn.create(tt);
            val ff = txn.commit();
            if (f == null) {
                return ff;
            } else {
                return f.thenComposeAsync(v -> ff);
            }
        }
    }

    private CompletableFuture<Void> saveQueueState(String queueName, TableBasedQueueInfo info) {
        try (val txn = storage.getMetadataStore().beginTransaction(false, queueName)) {
            txn.update(info);
            return txn.commit();
        }
    }

    @Override
    public void close() throws Exception {
        processorTask.cancel(true);
    }

    @Data
    @Builder
    @EqualsAndHashCode(callSuper = true)
    static public class TableBasedQueueInfo extends StorageMetadata {
        private String name;
        private boolean ignoreProcessing;
        private AtomicLong startTaskId;
        private AtomicLong endTaskId;
        private AtomicLong lastBatch;
        /**
         * Retrieves the key associated with the metadata.
         *
         * @return key.
         */
        @Override
        public String getKey() {
            return name;
        }

        /**
         * Creates a deep copy of this instance.
         *
         * @return A deep copy of this instance.
         */
        @Override
        public TableBasedQueueInfo deepCopy() {
            val retVal =  new TableBasedQueueInfoBuilder()
                    .name(this.name)
                    .ignoreProcessing(this.ignoreProcessing)
                    .startTaskId(new AtomicLong(this.startTaskId.get()))
                    .endTaskId(new AtomicLong(this.endTaskId.get()))
                    .lastBatch(new AtomicLong(this.lastBatch.get()))
                    .build();
            return retVal;
        }

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TableBasedQueueInfoBuilder implements ObjectBuilder<TableBasedQueueInfo> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<TableBasedQueueInfo, TableBasedQueueInfo.TableBasedQueueInfoBuilder> {
            @Override
            protected TableBasedQueueInfo.TableBasedQueueInfoBuilder newBuilder() {
                return TableBasedQueueInfo.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TableBasedQueueInfo object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.name);
                output.writeBoolean(object.ignoreProcessing);
                output.writeCompactLong(object.startTaskId.get());
                output.writeCompactLong(object.endTaskId.get());
                output.writeCompactLong(object.lastBatch.get());
            }

            private void read00(RevisionDataInput input, TableBasedQueueInfo.TableBasedQueueInfoBuilder b) throws IOException {
                b.name(input.readUTF());
                b.ignoreProcessing(input.readBoolean());
                b.startTaskId(new AtomicLong(input.readCompactLong()));
                b.endTaskId(new AtomicLong(input.readCompactLong()));
                b.lastBatch(new AtomicLong(input.readCompactLong()));
            }
        }
    }

    @Data
    @Builder
    @EqualsAndHashCode(callSuper = true)
    static public class TableBasedTaskInfo extends StorageMetadata {
        private final static GarbageCollector.TaskInfo.AbstractTaskInfoSerializer SERIALIZER = new GarbageCollector.TaskInfo.AbstractTaskInfoSerializer();
        private String id;
        private GarbageCollector.AbstractTaskInfo taskInfo;

        /**
         * Retrieves the key associated with the metadata.
         *
         * @return key.
         */
        @Override
        public String getKey() {
            return id;
        }

        /**
         * Creates a deep copy of this instance.
         *
         * @return A deep copy of this instance.
         */
        @Override
        public TableBasedTaskInfo deepCopy() {
            val retVal = new TableBasedTaskInfoBuilder().build();
            retVal.setTaskInfo(((GarbageCollector.TaskInfo) taskInfo).toBuilder().build());
            return retVal;
        }


        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TableBasedTaskInfoBuilder implements ObjectBuilder<TableBasedTaskInfo> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<TableBasedTaskInfo, TableBasedTaskInfo.TableBasedTaskInfoBuilder> {
            @Override
            protected TableBasedTaskInfo.TableBasedTaskInfoBuilder newBuilder() {
                return TableBasedTaskInfo.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TableBasedTaskInfo object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.id);
                SERIALIZER.serialize(output, object.taskInfo);

            }

            private void read00(RevisionDataInput input, TableBasedTaskInfo.TableBasedTaskInfoBuilder b) throws IOException {
                b.id(input.readUTF());
                b.taskInfo(SERIALIZER.deserialize(input.getBaseStream()));
            }
        }
    }
}
