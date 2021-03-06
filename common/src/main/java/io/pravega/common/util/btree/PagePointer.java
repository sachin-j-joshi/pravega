/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;

/**
 * Pointer to a BTreePage.
 */
@Getter
class PagePointer {
    /**
     * Reserved value to indicate an offset has not yet been assigned.
     */
    static final long NO_OFFSET = -1L;
    private final ByteArraySegment key;
    private final long offset;
    private final int length;

    /**
     * Creates a new instance of the PagePointer class.
     *
     * @param key    A ByteArraySegment representing the key. If supplied (may be null), a copy of this will be kept.
     * @param offset Desired Pages's Offset.
     * @param length Desired Page's Length.
     */
    PagePointer(ByteArraySegment key, long offset, int length) {
        // Make a copy of the key. ByteArraySegments are views into another array (of the BTreePage). As such, if the
        // BTreePage is modified (in case the first key is deleted), this view may return a different set of bytes.
        this.key = key == null ? null : new ByteArraySegment(key.getCopy());
        this.offset = offset;
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("Offset = %s, Length = %s", this.offset, this.length);
    }
}
