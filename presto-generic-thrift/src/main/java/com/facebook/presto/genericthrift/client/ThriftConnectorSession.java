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
package com.facebook.presto.genericthrift.client;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftConnectorSession
{
    private final String queryId;
    private final String user;
    private final long startTime; // unix time in millis
    private final Map<String, ThriftSingleValue> properties;

    @ThriftConstructor
    public ThriftConnectorSession(
            String queryId,
            String user,
            long startTime,
            Map<String, ThriftSingleValue> properties)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.user = requireNonNull(user, "user is null");
        this.startTime = requireNonNull(startTime, "startTime is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @ThriftField(1)
    public String getQueryId()
    {
        return queryId;
    }

    @ThriftField(2)
    public String getUser()
    {
        return user;
    }

    @ThriftField(3)
    public long getStartTime()
    {
        return startTime;
    }

    @ThriftField(4)
    public Map<String, ThriftSingleValue> getProperties()
    {
        return properties;
    }

    public static ThriftConnectorSession fromConnectorSession(ConnectorSession session)
    {
        // TODO: populate session properties
        return new ThriftConnectorSession(session.getQueryId(), session.getUser(), session.getStartTime(), ImmutableMap.of());
    }
}
