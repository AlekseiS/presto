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
package com.facebook.presto.server.protocol;

import com.facebook.presto.client.CreateQuerySession;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.servlet.http.HttpServletRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public final class QueryRequestSessionContext
        implements SessionContext
{
    private static final Splitter PROPERTY_NAME_SPLITTER = Splitter.on('.');

    private final CreateQuerySession createQuerySession;
    private final Identity identity;
    private final String remoteUserAddress;
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    public QueryRequestSessionContext(CreateQuerySession createQuerySession, HttpServletRequest servletRequest)
    {
        this.createQuerySession = requireNonNull(createQuerySession, "createQuerySession is null");
        // TODO: throw web exception instead
        checkArgument(!isNullOrEmpty(createQuerySession.getUser()), "User must be set");
        this.identity = new Identity(createQuerySession.getUser(), Optional.ofNullable(servletRequest.getUserPrincipal()));
        this.remoteUserAddress = servletRequest.getRemoteAddr();

        // parse session properties
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Map<String, ImmutableMap.Builder<String, String>> catalogSessionProperties = new HashMap<>();
        if (createQuerySession.getProperties() != null && !createQuerySession.getProperties().isEmpty()) {
            for (Map.Entry<String, String> entry : createQuerySession.getProperties().entrySet()) {
                String fullPropertyName = entry.getKey();
                String propertyValue = entry.getValue();
                List<String> nameParts = PROPERTY_NAME_SPLITTER.splitToList(fullPropertyName);
                if (nameParts.size() == 1) {
                    String propertyName = nameParts.get(0);

                    // TODO: throw web exception
                    checkArgument(!propertyName.isEmpty(), "propertyName is empty");

                    // catalog session properties can not be validated until the transaction has stated, so we delay system property validation also
                    systemProperties.put(propertyName, propertyValue);
                }
                else if (nameParts.size() == 2) {
                    String catalogName = nameParts.get(0);
                    String propertyName = nameParts.get(1);

                    // TODO: throw web exception
                    checkArgument(!catalogName.isEmpty(), "catalogName is empty");
                    checkArgument(!propertyName.isEmpty(), "propertyName is empty");

                    // catalog session properties can not be validated until the transaction has stated
                    catalogSessionProperties.computeIfAbsent(catalogName, id -> ImmutableMap.builder()).put(propertyName, propertyValue);
                }
                else {
                    // TODO: throw web exception
                    throw new IllegalArgumentException("Invalid property name: " + fullPropertyName);
                }
            }
        }
        this.systemProperties = systemProperties.build();
        this.catalogSessionProperties = catalogSessionProperties.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build()));
    }

    @Override
    public Identity getIdentity()
    {
        return identity;
    }

    @Override
    public String getCatalog()
    {
        return createQuerySession.getCatalog();
    }

    @Override
    public String getSchema()
    {
        return createQuerySession.getSchema();
    }

    @Override
    public String getSource()
    {
        return createQuerySession.getSource();
    }

    @Override
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @Override
    public String getUserAgent()
    {
        return createQuerySession.getUserAgent();
    }

    @Override
    public String getClientInfo()
    {
        return createQuerySession.getClientInfo();
    }

    @Override
    public Set<String> getClientTags()
    {
        return createQuerySession.getClientTags() == null ? ImmutableSet.of() : createQuerySession.getClientTags();
    }

    @Override
    public String getTimeZoneId()
    {
        return createQuerySession.getTimeZoneId();
    }

    @Override
    public String getLanguage()
    {
        return createQuerySession.getLanguage();
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return createQuerySession.getPreparedStatements() == null ? ImmutableMap.of() : createQuerySession.getPreparedStatements();
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return createQuerySession.getTransactionId() == null ? Optional.empty() : Optional.of(TransactionId.valueOf(createQuerySession.getTransactionId()));
    }

    @Override
    public boolean supportClientTransaction()
    {
        return createQuerySession.supportClientTransaction();
    }
}
