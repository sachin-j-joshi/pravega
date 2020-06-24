/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.Permission;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.io.StreamHelpers;
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
import org.apache.http.HttpStatus;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 *  {@link ChunkStorage} for extended S3 based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * The concat operation is implemented as multi part copy.
 */

@Slf4j
public class ExtendedS3ChunkStorage extends BaseChunkStorage {

    //region members
    private final ExtendedS3StorageConfig config;
    private final S3Client client;

    //endregion

    //region constructor
    public ExtendedS3ChunkStorage(S3Client client, ExtendedS3StorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
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

    //region implementation

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
        if (!checkExists(chunkName)) {
            throw new ChunkNotFoundException(chunkName, "Chunk not found", new FileNotFoundException(chunkName));
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
        if (!checkExists(chunkName)) {
            throw new ChunkNotFoundException(chunkName, "Chunk not found", new FileNotFoundException(chunkName));
        }
        return ChunkHandle.writeHandle(chunkName);
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
        try {
            if (fromOffset < 0 || bufferOffset < 0 || length < 0) {
                throw new ArrayIndexOutOfBoundsException();
            }

            try (InputStream reader = client.readObjectStream(config.getBucket(),
                    config.getPrefix() + handle.getChunkName(), Range.fromOffsetLength(fromOffset, length))) {
                /*
                 * TODO: This implementation assumes that if S3Client.readObjectStream returns null, then
                 * the object does not exist and we throw StreamNotExistsException. The javadoc, however,
                 * says that this call returns null in case of 304 and 412 responses. We need to
                 * investigate what these responses mean precisely and react accordingly.
                 *
                 * See https://github.com/pravega/pravega/issues/1549
                 */
                if (reader == null) {
                    throw new ChunkNotFoundException(handle.getChunkName(), "Chunk not found", new FileNotFoundException(handle.getChunkName()));
                }

                int bytesRead = StreamHelpers.readAll(reader, buffer, bufferOffset, length);

                return bytesRead;
            }
        } catch (Exception e) {
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
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IndexOutOfBoundsException Throws IndexOutOfBoundsException in case of invalid index.
     */
    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException, IndexOutOfBoundsException {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        try {
            S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(), config.getPrefix() + handle.getChunkName());
            client.putObject(this.config.getBucket(), this.config.getPrefix() + handle.getChunkName(),
                    Range.fromOffsetLength(offset, length), data);
            return (int) length;
        } catch (Exception e) {
            throwException(handle.getChunkName(), "doWrite", e);
        }
        return 0;
    }

    /**
     * Concatenates two or more chunks using storage native  functionality. (Eg. Multipart upload.)
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be appended together. The chunks are appended in the same sequence the names are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        int totalBytesConcated = 0;
        try {
            int partNumber = 1;

            SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
            String targetPath = config.getPrefix() + chunks[0].getName();
            String uploadId = client.initiateMultipartUpload(config.getBucket(), targetPath);

            // check whether the target exists
            if (!checkExists(chunks[0].getName())) {
                throw new FileNotFoundException(chunks[0].getName());
            }

            //Copy the parts
            for (int i = 0; i < chunks.length; i++) {
                if (0 != chunks[i].getLength()) {
                    val sourceHandle = chunks[i];
                    S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getBucket(),
                            config.getPrefix() + sourceHandle.getName());
                    long objectSize = metadataResult.getContentLength(); // in bytes
                    Preconditions.checkState(objectSize >= chunks[i].getLength());
                    CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                            config.getPrefix() + sourceHandle.getName(),
                            config.getBucket(),
                            targetPath,
                            uploadId,
                            partNumber++).withSourceRange(Range.fromOffsetLength(0, chunks[i].getLength()));

                    CopyPartResult copyResult = client.copyPart(copyRequest);
                    partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));
                    totalBytesConcated += chunks[i].getLength();
                }
            }

            //Close the upload
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getBucket(),
                    targetPath, uploadId).withParts(partEtags));
            // Delete all source objects.
            for (int i = 1; i < chunks.length; i++) {
                client.deleteObject(config.getBucket(), config.getPrefix() + chunks[i].getName());
            }
        } catch (RuntimeException e) {
            throw e; // make spotbugs happy
        } catch (Exception e) {
            throwException(chunks[0].getName(), "doConcat", e);
        }
        return totalBytesConcated;
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
        try {
            setPermission(handle, isReadOnly ? Permission.READ : Permission.FULL_CONTROL);
        } catch (Exception e) {
            throwException(handle.getChunkName(), "doSetReadOnly", e);
        }
        return true;
    }

    private void setPermission(ChunkHandle handle, Permission permission) {
        AccessControlList acl = client.getObjectAcl(config.getBucket(), config.getPrefix() + handle.getChunkName());
        acl.getGrants().clear();
        acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), permission));

        client.setObjectAcl(
                new SetObjectAclRequest(config.getBucket(), config.getPrefix() + handle.getChunkName()).withAcl(acl));
    }

    private <T> T throwException(String chunkName, String message, Exception e) throws ChunkStorageException {
        if (e instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) e;
            String errorCode = Strings.nullToEmpty(s3Exception.getErrorCode());

            if (errorCode.equals("NoSuchKey")) {
                throw new ChunkNotFoundException(chunkName, "Chunk not found");
            }

            if (errorCode.equals("PreconditionFailed")) {
                throw new ChunkAlreadyExistsException(chunkName, "Chunk already exists");
            }

            if (errorCode.equals("InvalidRange")
                    || errorCode.equals("InvalidArgument")
                    || errorCode.equals("MethodNotAllowed")
                    || s3Exception.getHttpCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                throw new IllegalArgumentException(chunkName, e);
            }

            if (errorCode.equals("AccessDenied")) {
                throw new ChunkStorageException(chunkName, "Access denied", new AccessDeniedException(chunkName));
            }
        }

        if (e instanceof IndexOutOfBoundsException) {
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        throw Exceptions.sneakyThrow(e);
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
            S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                    config.getPrefix() + chunkName);

            AccessControlList acls = client.getObjectAcl(config.getBucket(), config.getPrefix() + chunkName);
            ChunkInfo information = ChunkInfo.builder()
                    .name(chunkName)
                    .length(result.getContentLength())
                    .build();

            return information;
        } catch (Exception e) {
            throwException(chunkName, "doGetInfo", e);
        }
        return null;
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
            if (!client.listObjects(config.getBucket(), config.getPrefix() + chunkName).getObjects().isEmpty()) {
                throw new ChunkAlreadyExistsException(chunkName, "Chunk already exists");
            }

            S3ObjectMetadata metadata = new S3ObjectMetadata();
            metadata.setContentLength((long) 0);

            PutObjectRequest request = new PutObjectRequest(config.getBucket(), config.getPrefix() + chunkName, null);

            AccessControlList acl = new AccessControlList();
            acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), Permission.FULL_CONTROL));
            request.setAcl(acl);

            if (config.isUseNoneMatch()) {
                request.setIfNoneMatch("*");
            }
            client.putObject(request);

            return ChunkHandle.writeHandle(chunkName);
        } catch (Exception e) {
            throwException(chunkName, "doCreate", e);
        }
        return null;
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
        try {
            S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                    config.getPrefix() + chunkName);
            return true;
        } catch (S3Exception e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return false;
            } else {
                throw e;
            }
        }
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
            client.deleteObject(config.getBucket(), config.getPrefix() + handle.getChunkName());
        } catch (Exception e) {
            throwException(handle.getChunkName(), "doDelete", e);
        }
    }

    //endregion

}
