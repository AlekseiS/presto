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

import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class JsonBasedSessionContext
        implements SessionContext
{
    public JsonBasedSessionContext(CreateQueryInfo queryInfo, Optional<Principal> userPrincipal)
    {

    }

    @Override
    public Identity getIdentity()
    {
        return null;
    }

    @Override
    public String getCatalog()
    {
        return null;
    }

    @Override
    public String getSchema()
    {
        return null;
    }

    @Override
    public String getSource()
    {
        return null;
    }

    @Override
    public String getRemoteUserAddress()
    {
        return null;
    }

    @Override
    public String getUserAgent()
    {
        return null;
    }

    @Override
    public String getClientInfo()
    {
        return null;
    }

    @Override
    public Set<String> getClientTags()
    {
        return ImmutableSet.of();
    }

    @Override
    public String getTimeZoneId()
    {
        return null;
    }

    @Override
    public String getLanguage()
    {
        return null;
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return Optional.empty();
    }

    @Override
    public boolean supportClientTransaction()
    {
        return false;
    }
}
