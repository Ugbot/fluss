/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.fs.azurenative;

/** Canonical (prefix-stripped) configuration keys recognized by the native Azure Blob plugin. */
final class AzureNativeSettings {

    /** The full Blob service endpoint, e.g. {@code https://<account>.blob.core.windows.net}. */
    static final String ENDPOINT = "endpoint";

    /**
     * Alias of {@link #ENDPOINT} for the blob service endpoint, matching the Azurite/SDK naming.
     */
    static final String BLOB_ENDPOINT = "blob.endpoint";

    /** The storage account name. */
    static final String ACCOUNT_NAME = "account.name";

    /** The storage account shared key. */
    static final String ACCOUNT_KEY = "account.key";

    /** A full connection string; if set, it takes precedence over endpoint/account/key. */
    static final String CONNECTION_STRING = "connection.string";

    private AzureNativeSettings() {}
}
