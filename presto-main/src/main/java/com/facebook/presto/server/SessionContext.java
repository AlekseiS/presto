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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Strings.isNullOrEmpty;

public class SessionContext
{
    private final String catalog;
    private final String schema;

    private final Identity identity;

    private final String source;
    private final String userAgent;
    private final String remoteUserAddress;
    private final String timeZoneId;
    private final String language;
    private final Set<String> clientTags;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    private final Map<String, String> preparedStatements;

    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String clientInfo;

    public SessionContext(
            String catalog,
            String schema,
            Identity identity,
            String source,
            String userAgent,
            String remoteUserAddress,
            String timeZoneId,
            String language,
            Set<String> clientTags,
            Map<String, String> systemProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Map<String, String> preparedStatements,
            Optional<TransactionId> transactionId,
            boolean clientTransactionSupport,
            String clientInfo)
    {
        assertRequest((catalog != null) || (schema == null), "schema is set but catalog is not");
        this.catalog = catalog;
        this.schema = schema;
        this.identity = assertNotNull(identity, "identity is null");
        this.source = source;
        this.userAgent = userAgent;
        assertRequest(!isNullOrEmpty(remoteUserAddress), "remoteUserAddress is null or empty");
        this.remoteUserAddress = remoteUserAddress;
        this.timeZoneId = timeZoneId;
        this.language = language;
        this.clientTags = ImmutableSet.copyOf(assertNotNull(clientTags, "clientTags is null"));
        this.systemProperties = ImmutableMap.copyOf(assertNotNull(systemProperties, "systemProperties is null"));
        // TODO: deep copy
        this.catalogSessionProperties = ImmutableMap.copyOf(assertNotNull(catalogSessionProperties, "catalogSessionProperties is null"));
        this.preparedStatements = ImmutableMap.copyOf(assertNotNull(preparedStatements, "preparedStatements is null"));
        this.transactionId = assertNotNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.clientInfo = clientInfo;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    @Nullable
    public String getCatalog()
    {
        return catalog;
    }

    @Nullable
    public String getSchema()
    {
        return schema;
    }

    @Nullable
    public String getSource()
    {
        return source;
    }

    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @Nullable
    public String getUserAgent()
    {
        return userAgent;
    }

    @Nullable
    public String getClientInfo()
    {
        return clientInfo;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @Nullable
    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    @Nullable
    public String getLanguage()
    {
        return language;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    public boolean supportClientTransaction()
    {
        return clientTransactionSupport;
    }

    private static void assertRequest(boolean expression, String message)
    {
        if (!expression) {
            throw badRequest(message);
        }
    }

    private static <T> T assertNotNull(T reference, String message)
    {
        if (reference == null) {
            throw badRequest(message);
        }
        return reference;
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(Response
                .status(Response.Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }
}
