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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.api.PrestoThriftConnectorSession;
import com.facebook.presto.connector.thrift.api.PrestoThriftPropertyMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSessionValue;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Client session properties are those supplied by thrift interface implementation.
 * Values for these properties are passed to thrift node in certain calls.
 * It's up to thrift implementation to interpret those.
 */
public final class ThriftClientSessionProperties
{
    private final PrestoThriftServiceProvider clientProvider;
    private final TypeManager typeManager;
    private List<PropertyMetadata<?>> properties;

    @Inject
    public ThriftClientSessionProperties(PrestoThriftServiceProvider clientProvider, TypeManager typeManager)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        this.properties = clientProvider.runOnAnyHost(PrestoThriftService::listSessionProperties)
                .stream()
                .map(this::toPropertyMetadata)
                .collect(toImmutableList());
        return properties;
    }

    public Map<String, PrestoThriftSessionValue> getSessionValues(ConnectorSession session)
    {
        checkState(properties != null, "properties must be loaded");
        if (properties.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, PrestoThriftSessionValue> result = new HashMap<>(properties.size());
        for (PropertyMetadata<?> property : properties) {
            Object value = session.getProperty(property.getName(), property.getJavaType());
            if (!Objects.equals(property.getDefaultValue(), value)) {
                result.put(property.getName(), toThriftSessionValue(value, property.getSqlType()));
            }
        }
        return result;
    }

    public static PrestoThriftConnectorSession toThriftSession(ConnectorSession session, ThriftClientSessionProperties clientSessionProperties)
    {
        return new PrestoThriftConnectorSession(session.getQueryId(), session.getUser(), session.getStartTime(), clientSessionProperties.getSessionValues(session));
    }

    private PropertyMetadata<?> toPropertyMetadata(PrestoThriftPropertyMetadata thriftProperty)
    {
        Type type = typeManager.getType(parseTypeSignature((thriftProperty.getType())));
        PrestoThriftSessionValue sessionValue = thriftProperty.getDefaultValue();
        switch (type.getTypeSignature().getBase()) {
            case StandardTypes.BIGINT:
                return longSessionProperty(thriftProperty.getName(), thriftProperty.getDescription(),
                        sessionValue.isNullValue() ? null : sessionValue.getLongValue(), thriftProperty.isHidden());
            case StandardTypes.INTEGER:
                return integerSessionProperty(thriftProperty.getName(), thriftProperty.getDescription(),
                        sessionValue.isNullValue() ? null : sessionValue.getIntValue(), thriftProperty.isHidden());
            case StandardTypes.BOOLEAN:
                return booleanSessionProperty(thriftProperty.getName(), thriftProperty.getDescription(),
                        sessionValue.isNullValue() ? null : sessionValue.getBooleanValue(), thriftProperty.isHidden());
            case StandardTypes.DOUBLE:
                return doubleSessionProperty(thriftProperty.getName(), thriftProperty.getDescription(),
                        sessionValue.isNullValue() ? null : sessionValue.getDoubleValue(), thriftProperty.isHidden());
            case StandardTypes.VARCHAR:
                return stringSessionProperty(thriftProperty.getName(), thriftProperty.getDescription(),
                        sessionValue.isNullValue() ? null : sessionValue.getStringValue(), thriftProperty.isHidden());
            default:
                throw new IllegalArgumentException("Unsupported type for session property: " + type);
        }
    }

    private static PrestoThriftSessionValue toThriftSessionValue(Object value, Type type)
    {
        if (value == null) {
            return new PrestoThriftSessionValue(true, null, null, null, null, null);
        }
        switch (type.getTypeSignature().getBase()) {
            case StandardTypes.BIGINT:
                return new PrestoThriftSessionValue(false, (long) value, null, null, null, null);
            case StandardTypes.INTEGER:
                return new PrestoThriftSessionValue(false, null, (int) value, null, null, null);
            case StandardTypes.BOOLEAN:
                return new PrestoThriftSessionValue(false, null, null, (boolean) value, null, null);
            case StandardTypes.DOUBLE:
                return new PrestoThriftSessionValue(false, null, null, null, (double) value, null);
            case StandardTypes.VARCHAR:
                return new PrestoThriftSessionValue(false, null, null, null, null, (String) value);
            default:
                throw new IllegalArgumentException("Unsupported type for session value: " + type);
        }
    }
}
