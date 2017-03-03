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
package com.facebook.presto.genericthrift;

import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftPropertyMetadata;
import com.facebook.presto.genericthrift.client.ThriftSessionValue;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
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
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class GenericThriftClientSessionProperties
{
    private final PrestoClientProvider clientProvider;
    private final TypeManager typeManager;
    private List<PropertyMetadata<?>> properties;

    @Inject
    public GenericThriftClientSessionProperties(PrestoClientProvider clientProvider, TypeManager typeManager)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    private List<PropertyMetadata<?>> loadSessionProperties()
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            List<ThriftPropertyMetadata> thriftSessionProperties = client.listSessionProperties();
            if (thriftSessionProperties.isEmpty()) {
                return ImmutableList.of();
            }
            List<PropertyMetadata<?>> result = thriftSessionProperties
                    .stream()
                    .map(thriftProperty -> {
                        Type type = typeManager.getType(parseTypeSignature(thriftProperty.getType()));
                        ThriftSessionValue sessionValue = thriftProperty.getDefaultValue();
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
                    })
                    .collect(toList());
            return unmodifiableList(result);
        }
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        this.properties = loadSessionProperties();
        return properties;
    }

    public Map<String, ThriftSessionValue> getSessionValues(ConnectorSession session)
    {
        checkState(properties != null, "properties must be loaded");
        if (properties.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, ThriftSessionValue> result = new HashMap<>(properties.size());
        for (PropertyMetadata<?> property : properties) {
            Object value = session.getProperty(property.getName(), property.getJavaType());
            if (!Objects.equals(property.getDefaultValue(), value)) {
                result.put(property.getName(), toThriftSessionValue(value, property.getSqlType()));
            }
        }
        return result;
    }

    private static ThriftSessionValue toThriftSessionValue(Object value, Type type)
    {
        if (value == null) {
            return new ThriftSessionValue(true, null, null, null, null, null);
        }
        switch (type.getTypeSignature().getBase()) {
            case StandardTypes.BIGINT:
                return new ThriftSessionValue(false, (long) value, null, null, null, null);
            case StandardTypes.INTEGER:
                return new ThriftSessionValue(false, null, (int) value, null, null, null);
            case StandardTypes.BOOLEAN:
                return new ThriftSessionValue(false, null, null, (boolean) value, null, null);
            case StandardTypes.DOUBLE:
                return new ThriftSessionValue(false, null, null, null, (double) value, null);
            case StandardTypes.VARCHAR:
                return new ThriftSessionValue(false, null, null, null, null, (String) value);
            default:
                throw new IllegalArgumentException("Unsupported type for session value: " + type);
        }
    }
}
