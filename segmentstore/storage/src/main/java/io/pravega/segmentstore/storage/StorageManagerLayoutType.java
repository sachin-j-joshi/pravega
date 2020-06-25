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

/**
 * Type of Storage metadata layout to use.
 */
public enum StorageManagerLayoutType {
    /**
     * Uses RollingStorage based layout.
     */
    LEGACY,

    /**
     * Uses layout that stores data in table segments.
     */
    TABLE_BASED,
}

