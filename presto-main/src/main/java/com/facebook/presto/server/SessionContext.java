/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface SessionContext
{
    @NotNull
    Identity getIdentity();

    @Nullable
    String getCatalog();

    @Nullable
    String getSchema();

    @Nullable
    String getSource();

    @NotNull
    String getRemoteUserAddress();

    @Nullable
    String getUserAgent();

    @Nullable
    String getClientInfo();

    @NotNull
    Set<String> getClientTags();

    @Nullable
    String getTimeZoneId();

    @Nullable
    String getLanguage();

    @NotNull
    Map<String, String> getSystemProperties();

    @NotNull
    Map<String, Map<String, String>> getCatalogSessionProperties();

    @NotNull
    Map<String, String> getPreparedStatements();

    @NotNull
    Optional<TransactionId> getTransactionId();

    boolean supportClientTransaction();
}
