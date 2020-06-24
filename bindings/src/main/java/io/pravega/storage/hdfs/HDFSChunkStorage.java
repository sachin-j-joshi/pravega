/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

/***
 *  {@link ChunkStorage} for HDFS based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * The concat operation is implemented using HDFS native concat operation.
 */

@Slf4j
class HDFSChunkStorage extends BaseChunkStorage {
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);

    //region Members

    private final HDFSStorageConfig config;
    private FileSystem fileSystem;
    private final AtomicBoolean closed;
    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config   The configuration to use.
     */
    HDFSChunkStorage(HDFSStorageConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.closed = new AtomicBoolean(false);
        initialize();
    }

    //endregion

    //region capabilities

    /**
     * Gets a value indicating whether this Storage implementation supports merge operation either natively or through appends.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsConcat() {
        return true;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsAppend() {
        return true;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsTruncation() {
        return false;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.fileSystem != null) {
                try {
                    this.fileSystem.close();
                    this.fileSystem = null;
                } catch (IOException e) {
                    log.warn("Could not close the HDFS filesystem: {}.", e);
                }
            }
        }
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        try {
            FileStatus last = fileSystem.getFileStatus(getFilePath(chunkName));
            ChunkInfo result = ChunkInfo.builder().name(chunkName).length(last.getLen()).build();
            return result;
        } catch (IOException e) {
            throwException(chunkName, "doGetInfo", e);
        }
        return null;
    }

    /**
     * Creates a new file.
     *
     * @param chunkName String name of the storage object to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        try {
            Path fullPath = getFilePath(chunkName);
            // Create the file, and then immediately close the returned OutputStream, so that HDFS may properly create the file.
            this.fileSystem.create(fullPath, READWRITE_PERMISSION, false, 0, this.config.getReplication(),
                    this.config.getBlockSize(), null).close();
            log.debug("Created '{}'.", fullPath);

            // return handle
            return ChunkHandle.writeHandle(chunkName);
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
            throw new ChunkAlreadyExistsException(chunkName, "HDFSChunkStorage::doCreate");
        } catch (IOException e) {
            throwException(chunkName, "doCreate", e);
        }
        return null;
    }

    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the storage object to check.
     * @return True if the object exists, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        FileStatus status = null;
        try {
            status = fileSystem.getFileStatus(getFilePath(chunkName));
        } catch (IOException e) {
            // HDFS could not find the file. Returning false.
            log.warn("Got exception checking if file exists", e);
        }
        boolean exists = status != null;
        return exists;
    }

    /**
     * Deletes a file.
     *
     * @param handle ChunkHandle of the storage object to delete.
     * @return True if the object was deleted, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        try {
            boolean retValue = this.fileSystem.delete(getFilePath(handle.getChunkName()), true);
        } catch (IOException e) {
            throwException(handle.getChunkName(), "doDelete", e);
        }
    }

    private void throwException(String chunkName, String message, IOException e) throws ChunkStorageException {
        if (e instanceof FileNotFoundException) {
            throw new ChunkNotFoundException(chunkName, message, e);
        }
        throw new ChunkStorageException(chunkName, message, e);
    }

    /**
     * Opens storage object for Read.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        try {
            FileStatus status = fileSystem.getFileStatus(getFilePath(chunkName));
        } catch (IOException e) {
            throwException(chunkName, "doOpenRead", e);
        }
        return ChunkHandle.readHandle(chunkName);
    }

    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        try {
            FileStatus status = fileSystem.getFileStatus(getFilePath(chunkName));
            return ChunkHandle.writeHandle(chunkName);
        } catch (IOException e) {
            throwException(chunkName, "doOpenWrite", e);
        }
        return null;
    }

    /**
     * Reads a range of bytes from the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to read from.
     * @param fromOffset Offset in the file from which to start reading.
     * @param length Number of bytes to read.
     * @param buffer Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws NullPointerException  If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        ensureInitializedAndNotClosed();
        try {

            if (fromOffset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
                throw new ArrayIndexOutOfBoundsException(String.format(
                        "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                        fromOffset, bufferOffset, length, buffer.length));
            }

            int totalBytesRead = readInternal(handle, buffer, fromOffset, bufferOffset, length);
            return totalBytesRead;
        } catch (IOException e) {
            throwException(handle.getChunkName(), "doRead", e);
        }
        return 0;
    }

    /**
     * Writes the given data to the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to write to.
     * @param offset Offset in the file to start writing.
     * @param length Number of bytes to write.
     * @param data An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IndexOutOfBoundsException Throws IndexOutOfBoundsException in case of invalid index.
     */
    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException, IndexOutOfBoundsException {
        ensureInitializedAndNotClosed();
        try (FSDataOutputStream stream = this.fileSystem.append(getFilePath(handle.getChunkName()))) {
            if (stream.getPos() != offset) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                throw new IndexOutOfBoundsException();
            }

            if (length == 0) {
                // Note: IOUtils.copyBytes with length == 0 will enter an infinite loop, hence the need for this check.
                return 0;
            }

            // We need to be very careful with IOUtils.copyBytes. There are many overloads with very similar signatures.
            // There is a difference between (InputStream, OutputStream, int, boolean) and (InputStream, OutputStream, long, boolean),
            // in that the one with "int" uses the third arg as a buffer size, and the one with "long" uses it as the number
            // of bytes to copy.
            IOUtils.copyBytes(data, stream, (long) length, false);

            stream.flush();
        } catch (IOException e) {
            throwException(handle.getChunkName(), "doWrite", e);
        }
        return length;
    }

    /**
     * Concatenates two or more chunks using storage native  functionality. (Eg. Multipart upload.)
     *
     * @param chunks Array of ChunkHandle to existing chunks to be appended together. The chunks are appended in the same sequence the names are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        ensureInitializedAndNotClosed();
        int length = 0;
        try {
            val sources = new Path[chunks.length - 1];
            for (int i = 1; i < chunks.length; i++) {
                val chunkLength = chunks[i].getLength();
                this.fileSystem.truncate(getFilePath(chunks[i].getName()), chunkLength);
                sources[i - 1] = getFilePath(chunks[i].getName());
                length += chunkLength;
            }
            // Concat source file into target.
            this.fileSystem.concat(getFilePath(chunks[0].getName()), sources);
        } catch (IOException e) {
            throwException(chunks[0].getName(), "doConcat", e);
        }
        return length;
    }

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException {
        throw new UnsupportedOperationException(getClass().getName() + " does not support chunk truncation.");
    }

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle ChunkHandle of the storage object.
     * @param isReadOnly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    protected boolean doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {
        try {
            this.fileSystem.setPermission(getFilePath(handle.getChunkName()), isReadOnly ? READONLY_PERMISSION : READWRITE_PERMISSION);
            return true;
        } catch (IOException e) {
            throwException(handle.getChunkName(), "doSetReadOnly", e);
        }
        return false;
    }

    //endregion

    //region Storage Implementation

    @SneakyThrows(IOException.class)
    public void initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", this.config.getHdfsHostURL());
        conf.set("fs.default.fs", this.config.getHdfsHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // FileSystem has a bad habit of caching Clients/Instances based on target URI. We do not like this, since we
        // want to own our implementation so that when we close it, we don't interfere with others.
        conf.set("fs.hdfs.impl.disable.cache", "true");
        if (!this.config.isReplaceDataNodesOnFailure()) {
            // Default is DEFAULT, so we only set this if we want it disabled.
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        }

        this.fileSystem = openFileSystem(conf);
        log.info("Initialized (HDFSHost = '{}'", this.config.getHdfsHostURL());
    }

    FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }
    //endregion

    //region Helpers
    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem != null, "HDFSStorage is not initialized.");
    }

    //endregion

    //Region HDFS helper methods.

    /**
     * Gets an HDFS-friendly path prefix for the given chunk name by pre-pending the HDFS root from the config.
     */
    private String getPathPrefix(String chunkName) {
        return this.config.getHdfsRoot() + Path.SEPARATOR + chunkName;
    }

    /**
     * Gets the full HDFS Path to a file for the given chunk, startOffset and epoch.
     */
    private Path getFilePath(String chunkName) {
        Preconditions.checkState(chunkName != null && chunkName.length() > 0, "chunkName must be non-null and non-empty");
        return new Path(getPathPrefix(chunkName));
    }

    /**
     * Determines whether the given FileStatus indicates the file is read-only.
     *
     * @param fs The FileStatus to check.
     * @return True or false.
     */
    private boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    private int readInternal(ChunkHandle handle, byte[] buffer, long offset, int bufferOffset, int length) throws IOException {
        //There is only one file per chunkName.
        try (FSDataInputStream stream = this.fileSystem.open(getFilePath(handle.getChunkName()))) {
            stream.readFully(offset, buffer, bufferOffset, length);
        } catch (EOFException e) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the current size of chunk.", offset));
        }
        return length;
    }

    //endregion
}