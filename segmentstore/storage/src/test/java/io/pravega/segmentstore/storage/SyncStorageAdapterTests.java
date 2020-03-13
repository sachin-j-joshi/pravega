/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

import lombok.Data;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class SyncStorageAdapterTests  {

    public static final int CONTAINER_EPOCH = 111;
    public static final int THREAD_COUNT = 3;

    @Test
    public void TestInitialized() {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        verify(mockSyncStorage).initialize(CONTAINER_EPOCH);
    }

    @Test
    public void TestClose() {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);
        adapter.close();
        adapter.close();
        verify(mockSyncStorage, times(1)).close();
    }


    @Test
    public void TestCreate() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);
        String testName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(11);
        SegmentHandle segmentHandle = new TestHandle(testName, false);
        when(mockSyncStorage.create(testName, policy)).thenReturn(segmentHandle);

        val ret = adapter.create(testName, policy, null).join();
        Assert.assertEquals(segmentHandle, ret);
        verify(mockSyncStorage).create(testName, policy);
    }

    @Test
    public void TestCreateThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);
        String testName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(11);
        Exception expected = new StreamSegmentNotExistsException(testName);
        when(mockSyncStorage.create(testName, policy)).thenThrow(expected);
        try {
            val ret = adapter.create(testName, policy, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).create(testName, policy);
    }

    @Test
    public void TestOpenWrite() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);
        when(mockSyncStorage.openWrite(testName)).thenReturn(segmentHandle);

        val ret = adapter.openWrite(testName).join();
        Assert.assertEquals(segmentHandle, ret);
        verify(mockSyncStorage).openWrite(testName);
    }

    @Test
    public void TestOpenWriteThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        Exception expected = new StreamSegmentNotExistsException(testName);
        when(mockSyncStorage.openWrite(testName)).thenThrow(expected);

        try {
            val ret = adapter.openWrite(testName).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).openWrite(testName);
    }

    @Test
    public void TestOpenRead() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, true);
        when(mockSyncStorage.openRead(anyString())).thenReturn(segmentHandle);

        val ret = adapter.openRead(testName).join();
        Assert.assertEquals(segmentHandle, ret);
        verify(mockSyncStorage).openRead(testName);
    }

    @Test
    public void TestOpenReadThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        Exception expected = new StreamSegmentNotExistsException(testName);
        when(mockSyncStorage.openRead(anyString())).thenThrow(expected);
        try {
            val ret = adapter.openRead(testName).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).openRead(testName);
    }


    @Test
    public void TestExists() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        when(mockSyncStorage.exists(anyString())).thenReturn(false);

        val ret = adapter.exists(testName, null).join();
        Assert.assertEquals(false, ret.booleanValue());
        verify(mockSyncStorage).exists(testName);
    }

    @Test
    public void TestExistsThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        Exception expected = new NullPointerException();
        when(mockSyncStorage.exists(anyString())).thenThrow(expected);

        try {
            val ret = adapter.exists(testName, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).exists(testName);
    }

    @Test
    public void TestGetStreamSegmentInfo() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentProperties info = StreamSegmentInformation.builder().name(testName).build();
        when(mockSyncStorage.getStreamSegmentInfo(anyString())).thenReturn(info);

        val ret = adapter.getStreamSegmentInfo(testName, null).join();
        Assert.assertEquals(info, ret);
        verify(mockSyncStorage).getStreamSegmentInfo(testName);
    }

    @Test
    public void TestGetStreamSegmentInfoThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        Exception expected = new NullPointerException();
        when(mockSyncStorage.getStreamSegmentInfo(anyString())).thenThrow(expected);
        try {
            val ret = adapter.getStreamSegmentInfo(testName, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).getStreamSegmentInfo(testName);
    }

    @Test
    public void TestSeal() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);

        val ret = adapter.seal(segmentHandle, null).join();
        verify(mockSyncStorage).seal(segmentHandle);
    }

    @Test
    public void TestSealThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);

        // Setup mock.
        Exception expected = new StreamSegmentNotExistsException(testName);
        doThrow(expected).when(mockSyncStorage).seal(segmentHandle);
        try {
            val ret = adapter.seal(segmentHandle, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).seal(segmentHandle);
    }

    @Test
    public void TestTruncate() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);

        val ret = adapter.truncate(segmentHandle, 12, null).join();
        verify(mockSyncStorage).truncate(segmentHandle, 12);
    }

    @Test
    public void TestTruncateThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);

        // Setup mock.
        Exception expected = new StreamSegmentNotExistsException(testName);
        doThrow(expected).when(mockSyncStorage).truncate(segmentHandle, 12);
        try {
            val ret = adapter.truncate(segmentHandle, 12, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).truncate(segmentHandle, 12);
    }

    @Test
    public void TestConcat() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String sourceName = "source";
        String targetName = "target";
        SegmentHandle segmentHandle = new TestHandle(targetName, false);

        val ret = adapter.concat(segmentHandle, 12, sourceName, null).join();
        verify(mockSyncStorage).concat(segmentHandle, 12, sourceName);
    }

    @Test
    public void TestConcatThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        String sourceName = "source";
        String targetName = "target";
        SegmentHandle segmentHandle = new TestHandle(targetName, false);

        // Setup mock.
        Exception expected = new StreamSegmentNotExistsException(targetName);
        doThrow(expected).when(mockSyncStorage).concat(segmentHandle, 12, sourceName);

        try {
            val ret = adapter.concat(segmentHandle, 12, sourceName, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }

        verify(mockSyncStorage).concat(segmentHandle, 12, sourceName);
    }

    @Test
    public void TestDelete() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);
        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, true);

        val ret = adapter.delete(segmentHandle, null).join();
        verify(mockSyncStorage).delete(segmentHandle);
    }

    @Test
    public void TestDeleteThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        // Setup mock.
        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, true);
        Exception expected = new StreamSegmentNotExistsException(testName);
        doThrow(expected).when(mockSyncStorage).delete(segmentHandle);

        try {
            val ret = adapter.delete(segmentHandle, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }

        verify(mockSyncStorage).delete(segmentHandle);
    }

    @Test
    public void TestWrite() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        // Parameters.
        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);
        InputStream is = new ByteArrayInputStream(new byte[1]);

        val ret = adapter.write(segmentHandle, 10, is, THREAD_COUNT, null).join();

        verify(mockSyncStorage).write(segmentHandle, 10, is, THREAD_COUNT);
    }

    @Test
    public void TestWriteThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        // Parameters.
        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, false);
        InputStream is = new ByteArrayInputStream(new byte[1]);

        // Setup mock.
        Exception expected = new StreamSegmentNotExistsException(testName);
        doThrow(expected).when(mockSyncStorage).write(segmentHandle, 10, is, THREAD_COUNT);

        try {
            val ret = adapter.write(segmentHandle, 10, is, THREAD_COUNT, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }
        verify(mockSyncStorage).write(segmentHandle, 10, is, THREAD_COUNT);
    }


    @Test
    public void TestRead() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        // Parameters.
        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, true);
        byte[] out = new byte[1];

        // Setup mock.
        when(mockSyncStorage.read(segmentHandle, 10, out, 0, THREAD_COUNT)).thenReturn(2);

        val ret = adapter.read(segmentHandle, 10, out, 0, THREAD_COUNT, null).join();

        // verify
        Assert.assertEquals(2, ret.intValue());
        verify(mockSyncStorage).read(segmentHandle, 10, out, 0, THREAD_COUNT);
    }

    @Test
    public void TestReadThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);

        // Parameters.
        String testName = "test";
        SegmentHandle segmentHandle = new TestHandle(testName, true);
        byte[] out = new byte[1];

        // Setup mock.
        Exception expected = new StreamSegmentNotExistsException(testName);
        when(mockSyncStorage.read(segmentHandle, 10, out, 0, THREAD_COUNT)).thenThrow(expected);

        try {
            val ret = adapter.read(segmentHandle, 10, out, 0, THREAD_COUNT, null).join();
            Assert.fail("Did not throw Exception as expected");
        } catch (CompletionException actual) {
            Assert.assertEquals(expected, actual.getCause());
        }

        verify(mockSyncStorage).read(segmentHandle, 10, out, 0, THREAD_COUNT);
    }

    @Test
    public void TestSupportsTruncation() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);
        when(mockSyncStorage.supportsTruncation()).thenReturn(false);

        val ret = adapter.supportsTruncation();
        Assert.assertEquals(false, ret);

        verify(mockSyncStorage).supportsTruncation();
    }

    @Test
    public void TestSupportsTruncationThrowsException() throws Exception {
        val mockSyncStorage = mock(SyncStorage.class);
        val adapter = new SyncStorageAdapter(mockSyncStorage, Executors.newScheduledThreadPool(THREAD_COUNT));
        adapter.initialize(CONTAINER_EPOCH);
        String testName = "test";
        Exception expected = new NullPointerException();
        when(mockSyncStorage.supportsTruncation()).thenThrow(expected);

        try {
            val ret = adapter.supportsTruncation();
            Assert.fail("Did not throw Exception as expected");
        } catch (Exception actual) {
            Assert.assertEquals(expected, actual);
        }

        verify(mockSyncStorage).supportsTruncation();
    }

    @Data
    private static class TestHandle implements SegmentHandle {
        private final String segmentName;
        private final boolean readOnly;
    }
}
