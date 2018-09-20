/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;

/**
 * This interface represents the credentials passed to Pravega for authentication and authorizing the access.
 *
 * All implementations must support Java serialization.
 */
public interface Credentials extends Serializable {
    /**
     * Returns the authentication type.
     * Pravega can support multiple authentication types in a single deployment.
     *
     * @return the authentication type expected by the these credentials.
     */
    String getAuthenticationType();

    /**
     * Returns the authorization token to be sent to Pravega.
     * @return A token in token68-compatible format (as defined by RFC 7235).
     */
    default String getAuthenticationToken() {
        throw new NotImplementedException("", "");
    }

    /**
     * Returns authorization parameters used by this specific authentication type.
     * @return The map of authentication headers and values.
     */
    default Map<String, String> getAuthParameters() {
        throw new RuntimeException("Deprecated");
    }
}
