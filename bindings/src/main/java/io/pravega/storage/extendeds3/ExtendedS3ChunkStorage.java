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
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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

import java.io.InputStream;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;

/**
 * {@link ChunkStorage} for extended S3 based storage.
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
    public ExtendedS3ChunkStorage(S3Client client, ExtendedS3StorageConfig config, Executor executor) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
    }
    //endregion

    //region capabilities

    @Override
    public boolean supportsConcat() {
        return true;
    }

    @Override
    public boolean supportsAppend() {
        return true;
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    //endregion

    //region implementation

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        if (!checkExists(chunkName)) {
            throw new ChunkNotFoundException(chunkName, "doOpenRead");
        }
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        if (!checkExists(chunkName)) {
            throw new ChunkNotFoundException(chunkName, "doOpenWrite");
        }

        AccessControlList acls = client.getObjectAcl(config.getBucket(), config.getPrefix() + chunkName);
        boolean canWrite = acls.getGrants().stream().anyMatch(grant -> grant.getPermission().compareTo(Permission.WRITE) >= 0);

        return new ChunkHandle(chunkName, !canWrite);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        try {
            try (InputStream reader = client.readObjectStream(config.getBucket(),
                    getObjectPath(handle.getChunkName()), Range.fromOffsetLength(fromOffset, length))) {
                if (reader == null) {
                    throw new ChunkNotFoundException(handle.getChunkName(), "doRead");
                }

                int bytesRead = StreamHelpers.readAll(reader, buffer, bufferOffset, length);

                return bytesRead;
            }
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        try {
            val objectPath = getObjectPath(handle.getChunkName());
            // Check object exists.
            client.getObjectMetadata(config.getBucket(), objectPath);

            // Put data.
            client.putObject(this.config.getBucket(), objectPath,
                    Range.fromOffsetLength(offset, length), data);
            return length;
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doWrite", e);
        }
    }

    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException {
        int totalBytesConcatenated = 0;
        String targetPath = getObjectPath(chunks[0].getName());
        String uploadId = null;
        boolean isCompleted = false;
        try {
            int partNumber = 1;

            SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
            uploadId = client.initiateMultipartUpload(config.getBucket(), targetPath);

            // check whether the target exists
            if (!checkExists(chunks[0].getName())) {
                throw new ChunkNotFoundException(chunks[0].getName(), "doConcat - Target segment does not exist");
            }

            //Copy the parts
            for (int i = 0; i < chunks.length; i++) {
                if (0 != chunks[i].getLength()) {
                    val sourceHandle = chunks[i];
                    S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getBucket(),
                            getObjectPath(sourceHandle.getName()));
                    long objectSize = metadataResult.getContentLength(); // in bytes
                    Preconditions.checkState(objectSize >= chunks[i].getLength());
                    CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                            getObjectPath(sourceHandle.getName()),
                            config.getBucket(),
                            targetPath,
                            uploadId,
                            partNumber++).withSourceRange(Range.fromOffsetLength(0, chunks[i].getLength()));

                    CopyPartResult copyResult = client.copyPart(copyRequest);
                    partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));
                    totalBytesConcatenated += chunks[i].getLength();
                }
            }

            //Close the upload
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getBucket(),
                    targetPath, uploadId).withParts(partEtags));
            isCompleted = true;
        } catch (RuntimeException e) {
            // Make spotbugs happy. Wants us to catch RuntimeException in a separate catch block.
            // Error message is REC_CATCH_EXCEPTION: Exception is caught when Exception is not thrown
            throw convertException(chunks[0].getName(), "doConcat", e);
        } catch (Exception e) {
            throw convertException(chunks[0].getName(), "doConcat", e);
        } finally {
            if (!isCompleted && null != uploadId) {
                client.abortMultipartUpload(new AbortMultipartUploadRequest(config.getBucket(), targetPath, uploadId));
            }
        }
        return totalBytesConcatenated;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException {
        try {
            setPermission(handle, isReadOnly ? Permission.READ : Permission.FULL_CONTROL);
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doSetReadOnly", e);
        }
    }

    private void setPermission(ChunkHandle handle, Permission permission) {
        AccessControlList acl = client.getObjectAcl(config.getBucket(), getObjectPath(handle.getChunkName()));
        acl.getGrants().clear();
        acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), permission));

        client.setObjectAcl(
                new SetObjectAclRequest(config.getBucket(), getObjectPath(handle.getChunkName())).withAcl(acl));
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        try {
            S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                    getObjectPath(chunkName));

            ChunkInfo information = ChunkInfo.builder()
                    .name(chunkName)
                    .length(result.getContentLength())
                    .build();

            return information;
        } catch (Exception e) {
            throw convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        try {
            if (!client.listObjects(config.getBucket(), getObjectPath(chunkName)).getObjects().isEmpty()) {
                throw new ChunkAlreadyExistsException(chunkName, "Chunk already exists");
            }

            S3ObjectMetadata metadata = new S3ObjectMetadata();
            metadata.setContentLength((long) 0);

            PutObjectRequest request = new PutObjectRequest(config.getBucket(), getObjectPath(chunkName), null);

            AccessControlList acl = new AccessControlList();
            acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), Permission.FULL_CONTROL));
            request.setAcl(acl);

            if (config.isUseNoneMatch()) {
                request.setIfNoneMatch("*");
            }
            client.putObject(request);

            return ChunkHandle.writeHandle(chunkName);
        } catch (Exception e) {
            throw convertException(chunkName, "doCreate", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        try {
            client.getObjectMetadata(config.getBucket(), getObjectPath(chunkName));
            return true;
        } catch (S3Exception e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return false;
            } else {
                throw convertException(chunkName, "checkExists", e);
            }
        }
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            // check whether the chunk exists
            if (!checkExists(handle.getChunkName())) {
                throw new ChunkNotFoundException(handle.getChunkName(), "doDelete");
            }
            client.deleteObject(config.getBucket(), getObjectPath(handle.getChunkName()));
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doDelete", e);
        }
    }

    private ChunkStorageException convertException(String chunkName, String message, Exception e)  {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) e;
            String errorCode = Strings.nullToEmpty(s3Exception.getErrorCode());

            if (errorCode.equals("NoSuchKey")) {
                retValue =  new ChunkNotFoundException(chunkName, message, e);
            }

            if (errorCode.equals("PreconditionFailed")) {
                retValue =  new ChunkAlreadyExistsException(chunkName, message, e);
            }

            if (errorCode.equals("InvalidRange")
                    || errorCode.equals("InvalidArgument")
                    || errorCode.equals("MethodNotAllowed")
                    || s3Exception.getHttpCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                throw new IllegalArgumentException(chunkName, e);
            }

            if (errorCode.equals("AccessDenied")) {
                retValue =  new ChunkStorageException(chunkName, String.format("Access denied for chunk %s - %s.", chunkName, message), e);
            }
        }

        if (retValue == null) {
            retValue = new ChunkStorageException(chunkName, message, e);
        }

        return retValue;
    }

    private String getObjectPath(String objectName) {
        return config.getPrefix() + objectName;
    }

    //endregion

}
