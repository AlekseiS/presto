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

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftTableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class ThriftTableMetadata
{
    private final SchemaTableName schemaTableName;
    private final List<ColumnMetadata> columns;
    private final Optional<String> comment;
    private final Set<Set<String>> indexableKeys;

    public ThriftTableMetadata(SchemaTableName schemaTableName, List<ColumnMetadata> columns, Optional<String> comment, Set<Set<String>> indexableKeys)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.indexableKeys = requireNonNull(indexableKeys, "indexableKeys is null");
    }

    public ThriftTableMetadata(PrestoThriftTableMetadata thriftTableMetadata, TypeManager typeManager)
    {
        this(thriftTableMetadata.getSchemaTableName().toSchemaTableName(),
                columnMetadata(thriftTableMetadata.getColumns(), typeManager),
                Optional.ofNullable(thriftTableMetadata.getComment()),
                thriftTableMetadata.getIndexableKeys() != null ? ImmutableSet.copyOf(thriftTableMetadata.getIndexableKeys()) : ImmutableSet.of());
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public boolean containsIndexableColumns(Set<ColumnHandle> indexableColumns)
    {
        Set<String> keyColumns = indexableColumns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableSet());
        return indexableKeys.contains(keyColumns);
    }

    public ConnectorTableMetadata toConnectorTableMetadata()
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                columns,
                ImmutableMap.of(),
                comment);
    }

    private static List<ColumnMetadata> columnMetadata(List<PrestoThriftColumnMetadata> columns, TypeManager typeManager)
    {
        return columns.stream()
                .map(column -> column.toColumnMetadata(typeManager))
                .collect(toImmutableList());
    }
}
