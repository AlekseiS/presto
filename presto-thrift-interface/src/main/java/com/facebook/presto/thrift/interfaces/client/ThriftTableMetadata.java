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
package com.facebook.presto.thrift.interfaces.client;

import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import java.util.List;

import static com.facebook.presto.thrift.interfaces.client.ThriftColumnMetadata.toColumnMetadata;
import static com.facebook.presto.thrift.interfaces.client.ThriftSchemaTableName.toSchemaTableName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThriftStruct
public final class ThriftTableMetadata
{
    private final ThriftSchemaTableName schemaTableName;
    private final List<ThriftColumnMetadata> columns;

    @ThriftConstructor
    public ThriftTableMetadata(
            @ThriftField(name = "schemaTableName") ThriftSchemaTableName schemaTableName,
            @ThriftField(name = "columns") List<ThriftColumnMetadata> columns)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @ThriftField(1)
    public ThriftSchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @ThriftField(2)
    public List<ThriftColumnMetadata> getColumns()
    {
        return columns;
    }

    public static ConnectorTableMetadata toConnectorTableMetadata(ThriftTableMetadata thriftTableMetadata, TypeManager typeManager)
    {
        return new ConnectorTableMetadata(toSchemaTableName(thriftTableMetadata.getSchemaTableName()),
                thriftTableMetadata.getColumns()
                        .stream()
                        .map(column -> toColumnMetadata(column, typeManager))
                        .collect(toList()));
    }
}
