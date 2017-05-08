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
package com.facebook.presto.connector.thrift.api;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftPropertyMetadata
{
    private final String name;
    private final String type;
    private final String description;
    private final PrestoThriftSessionValue defaultValue;
    private final boolean hidden;

    @ThriftConstructor
    public PrestoThriftPropertyMetadata(String name, String type, String description, PrestoThriftSessionValue defaultValue, boolean hidden)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.description = requireNonNull(description, "description is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        this.hidden = hidden;
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(2)
    public String getType()
    {
        return type;
    }

    @ThriftField(3)
    public String getDescription()
    {
        return description;
    }

    @ThriftField(4)
    public PrestoThriftSessionValue getDefaultValue()
    {
        return defaultValue;
    }

    @ThriftField(5)
    public boolean isHidden()
    {
        return hidden;
    }
}
