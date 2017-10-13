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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.server.SessionContext;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

public final class TestingSessionContext
{
    private TestingSessionContext() {}

    public static SessionContext fromSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new SessionContext(
                session.getCatalog().orElse(null),
                session.getSchema().orElse(null),
                session.getIdentity(),
                session.getSource().orElse(null),
                session.getUserAgent().orElse(null),
                session.getRemoteUserAddress().orElse(null),
                session.getTimeZoneKey().getId(),
                session.getLocale().getLanguage(),
                session.getClientTags(),
                session.getSystemProperties(),
                getCatalogSessionProperties(session),
                session.getPreparedStatements(),
                session.getTransactionId(),
                session.isClientTransactionSupport(),
                session.getClientInfo().orElse(null));
    }

    private static Map<String, Map<String, String>> getCatalogSessionProperties(Session session)
    {
        ImmutableMap.Builder<String, Map<String, String>> catalogSessionProperties = ImmutableMap.builder();
        for (Entry<ConnectorId, Map<String, String>> entry : session.getConnectorProperties().entrySet()) {
            catalogSessionProperties.put(entry.getKey().getCatalogName(), entry.getValue());
        }
        return catalogSessionProperties.build();
    }
}
