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

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class QueryRequestSessionContext
        implements SessionContext
{
    private final CreateQuerySession createQuerySession;
    private final Identity identity;
    private final String remoteUserAddress;

    public QueryRequestSessionContext(CreateQuerySession createQuerySession, HttpServletRequest servletRequest)
    {
        this.createQuerySession = requireNonNull(createQuerySession, "createQuerySession is null");
        // TODO: throw web exception instead
        checkArgument(!isNullOrEmpty(createQuerySession.getUser()), "User must be set");
        this.identity = new Identity(createQuerySession.getUser(), Optional.ofNullable(servletRequest.getUserPrincipal()));
        this.remoteUserAddress = servletRequest.getRemoteAddr();
    }

    @NotNull
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

    @NotNull
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
        return createQuerySession.getClientTags();
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
        return createQuerySession.getSystemProperties();
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return createQuerySession.getCatalogSessionProperties();
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return createQuerySession.getPreparedStatements();
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
