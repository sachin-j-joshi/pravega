/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Timer;
import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 *  {@link ChunkStorage} for file system based storage.
 *
 * Each Chunk is represented as a single file on the underlying storage.
 * The concat operation is implemented as append.
 *
 */

@Slf4j
public class FileSystemChunkStorage extends BaseChunkStorage {
    //region members

    private final FileSystemStorageConfig config;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FileSystemChunkStorage class.
     *
     * @param config   The configuration to use.
     */
    public FileSystemChunkStorage(FileSystemStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
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

    //region

    @VisibleForTesting
    protected FileChannel getFileChannel(Path path, StandardOpenOption openOption) throws IOException {
        try {
            return FileChannel.open(path, openOption);
        } catch (NoSuchFileException e) {
            throw new FileNotFoundException(path.toString());
        }
    }

    @VisibleForTesting
    protected long getFileSize(Path path) throws IOException {
        try {
            return Files.size(path);
        } catch (NoSuchFileException e) {
            throw new FileNotFoundException(path.toString());
        }
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        try {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getRoot(), chunkName),
                    PosixFileAttributes.class);
            ChunkInfo information = ChunkInfo.builder()
                    .name(chunkName)
                    .length(attrs.size())
                    //.lastModified(new ImmutableDate(attrs.creationTime().toMillis()))
                    .build();

            return information;
        } catch (IOException e) {
            throwExeption(chunkName, "doGetInfo", e);
        }
        throw new ChunkStorageException(chunkName, "Unreachable code");
    }

    /**
     * Creates a new file.
     *
     * @param chunkName String name of the storage object to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        try {
            FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemUtils.READ_WRITE_PERMISSION);

            Path path = Paths.get(config.getRoot(), chunkName);
            Path parent = path.getParent();
            assert parent != null;
            Files.createDirectories(parent);
            Files.createFile(path, fileAttributes);

        } catch (IOException e) {
            throwExeption(chunkName, "doCreate", e);
        }

        return ChunkHandle.writeHandle(chunkName);
    }

    public void throwExeption(String chunkName, String message, Exception e) throws ChunkStorageException {
        if (e instanceof FileNotFoundException || e instanceof NoSuchFileException) {
            throw new ChunkNotFoundException(chunkName, message);
        }
        if (e instanceof FileAlreadyExistsException) {
            throw new ChunkAlreadyExistsException(chunkName, message);
        }
        throw new ChunkStorageException(chunkName, message, e);
    }

    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the storage object to check.
     * @return True if the object exists, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        return Files.exists(Paths.get(config.getRoot(), chunkName));
    }

    /**
     * Deletes a file.
     *
     * @param handle ChunkHandle of the storage object to delete.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException, IllegalArgumentException {
        try {
            Files.delete(Paths.get(config.getRoot(), handle.getChunkName()));
        } catch (IOException e) {
            throwExeption(handle.getChunkName(), "doDelete", e);
        }
    }

    /**
     * Opens storage object for Read.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Path path = Paths.get(config.getRoot(), chunkName);

        if (!Files.exists(path)) {
            throw new ChunkNotFoundException(chunkName, "FileSystemChunkStorage::doOpenRead");
        }

        return ChunkHandle.readHandle(chunkName);
    }

    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Path path = Paths.get(config.getRoot(), chunkName);
        if (!Files.exists(path)) {
            throw new ChunkNotFoundException(chunkName, "FileSystemChunkStorage::doOpenWrite");
        } else if (Files.isWritable(path)) {
            return ChunkHandle.writeHandle(chunkName);
        } else {
            return ChunkHandle.readHandle(chunkName);
        }
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
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws NullPointerException  If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        Timer timer = new Timer();

        Path path = Paths.get(config.getRoot(), handle.getChunkName());
        try {
            long fileSize = getFileSize(path);
            if (fileSize < fromOffset) {
                throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                        "current size of chunk (%d).", fromOffset, fileSize));
            }
        } catch (IOException e) {
            throwExeption(handle.getChunkName(), "doRead", e);
        }

        try (FileChannel channel = getFileChannel(path, StandardOpenOption.READ)) {
            int totalBytesRead = 0;
            long readOffset = fromOffset;
            do {
                ByteBuffer readBuffer = ByteBuffer.wrap(buffer, bufferOffset, length);
                int bytesRead = channel.read(readBuffer, readOffset);
                bufferOffset += bytesRead;
                totalBytesRead += bytesRead;
                length -= bytesRead;
                readOffset += bytesRead;
            } while (length != 0);
            return totalBytesRead;
        } catch (IOException e) {
            throwExeption(handle.getChunkName(), "doRead", e);
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
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IndexOutOfBoundsException Throws IndexOutOfBoundsException in case of invalid index.
     */
    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        Timer timer = new Timer();

        if (handle.isReadOnly()) {
            throw new IllegalArgumentException("Write called on a readonly handle of chunk " + handle.getChunkName());
        }

        Path path = Paths.get(config.getRoot(), handle.getChunkName());

        long totalBytesWritten = 0;
        try (FileChannel channel = getFileChannel(path, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            if (fileSize != offset) {
                throw new IndexOutOfBoundsException(String.format("fileSize (%d) did not match offset (%d) for chunk %s", fileSize, offset, handle.getChunkName()));
            }

            // Wrap the input data into a ReadableByteChannel, but do not close it. Doing so will result in closing
            // the underlying InputStream, which is not desirable if it is to be reused.
            ReadableByteChannel sourceChannel = Channels.newChannel(data);
            while (length != 0) {
                long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                assert bytesWritten > 0 : "Unable to make any progress transferring data.";
                offset += bytesWritten;
                totalBytesWritten += bytesWritten;
                length -= bytesWritten;
            }
            channel.force(false);
        } catch (IOException e) {
            throwExeption(handle.getChunkName(), "doWrite", e);
        }
        return (int) totalBytesWritten;
    }

    /**
     * Concatenates two or more chunks using appends.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be appended together. The chunks are appended in the same sequence the names are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        try {
            int totalBytesConcated = 0;
            Path targetPath = Paths.get(config.getRoot(), chunks[0].getName());
            long offset = chunks[0].getLength();

            for (int i = 1; i < chunks.length; i++) {
                val source = chunks[i];
                Preconditions.checkArgument(!chunks[0].getName().equals(source.getName()), "target and source can not be same.");
                Path sourcePath = Paths.get(config.getRoot(), source.getName());
                long length = chunks[i].getLength();
                Preconditions.checkState(offset <= getFileSize(targetPath));
                Preconditions.checkState(length <= getFileSize(sourcePath));
                try (FileChannel targetChannel = getFileChannel(targetPath, StandardOpenOption.WRITE);
                     RandomAccessFile sourceFile = new RandomAccessFile(String.valueOf(sourcePath), "r")) {
                    while (length > 0) {
                        long bytesTransferred = targetChannel.transferFrom(sourceFile.getChannel(), offset, length);
                        offset += bytesTransferred;
                        length -= bytesTransferred;
                    }
                    targetChannel.force(false);
                    Files.delete(sourcePath);
                    totalBytesConcated += length;
                    offset += length;
                }

            }
            return totalBytesConcated;
        } catch (IOException e) {
            throwExeption(chunks[0].getName(), "doConcat", e);
        }
        return 0;
    }

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle ChunkHandle of the storage object.
     * @param isReadOnly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    protected boolean doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {
        Path path = null;
        try {
            path = Paths.get(config.getRoot(), handle.getChunkName());
            Files.setPosixFilePermissions(path, isReadOnly ? FileSystemUtils.READ_ONLY_PERMISSION : FileSystemUtils.READ_WRITE_PERMISSION);
            return true;
        } catch (IOException e) {
            throwExeption(path.toString(), "doSetReadOnly", e);
        }
        return false;
    }

    //endregion
}
