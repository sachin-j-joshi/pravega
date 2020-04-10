/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.segmentstore.storage.mocks.AbstractInMemoryChunkStorageProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NoOp implementation.
 */
@Slf4j
public class NoOpChunkStorageProvider extends AbstractInMemoryChunkStorageProvider {
    @Getter
    @Setter
    ConcurrentHashMap<String, ChunkData> chunkMetadata = new ConcurrentHashMap<>();

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorageProvider::doGetInfo");
        }
        return ChunkInfo.builder().name(chunkName).length(chunkData.length).build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null != chunkData) {
            throw new ChunkAlreadyExistsException(chunkName, "NoOpChunkStorageProvider::doCreate");
        }
        chunkMetadata.put(chunkName, new ChunkData());
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean doesExist(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        return chunkMetadata.containsKey(chunkName);
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException, IllegalArgumentException {
        Preconditions.checkNotNull(null != handle, "handle");
        Preconditions.checkNotNull(handle.getChunkName(), "handle");
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorageProvider::doDelete");
        }
        if (chunkData.isReadonly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        chunkMetadata.remove(handle.getChunkName());
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorageProvider::doOpenRead");
        }
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorageProvider::doOpenWrite");
        }
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorageProvider::doRead");
        }

        if (fromOffset >= chunkData.length || fromOffset + length > chunkData.length) {
            throw new IndexOutOfBoundsException("fromOffset");
        }

        if (fromOffset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    fromOffset, bufferOffset, length, buffer.length));
        }

        return length;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException, IndexOutOfBoundsException {

        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorageProvider::doWrite");
        }
        if (offset != chunkData.length) {
            throw new IndexOutOfBoundsException("");
        }
        if (chunkData.isReadonly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        chunkData.length = offset + length;
        chunkMetadata.put(handle.getChunkName(), chunkData);
        return length;
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        int total = 0;
        for (ConcatArgument chunk : chunks) {
            val chunkData = chunkMetadata.get(chunk.getName());
            Preconditions.checkState(null != chunkData);
            Preconditions.checkState(chunkData.length >= chunk.getLength());
            total += chunk.getLength();
        }

        val targetChunkData = chunkMetadata.get(chunks[0].getName());
        targetChunkData.length = total;
        for (int i = 1; i < chunks.length; i++) {
            chunkMetadata.remove(chunks[i].getName());
        }

        return total;
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException {
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorageProvider::doTruncate");
        }
        if (offset < chunkData.length) {
            chunkData.length = offset;
            return true;
        }
        return false;
    }

    @Override
    protected boolean doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {
        Preconditions.checkNotNull(null != handle, "handle");
        Preconditions.checkNotNull(handle.getChunkName(), "handle");
        String chunkName = handle.getChunkName();
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorageProvider::doSetReadOnly");
        }
        chunkData.isReadonly = isReadOnly;
        return false;
    }

    @Override
    public void addChunk(String chunkName, long length) {
        ChunkData chunkData = new ChunkData();
        chunkData.length = length;
        chunkMetadata.put(chunkName, chunkData);
    }

    /**
     * Stores the chunk data.
     */
    private static class ChunkData {
        private long length;
        private boolean isReadonly;
    }
}