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

import com.facebook.presto.genericthrift.client.ThriftNullableTableMetadata;
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftSchemaTableName;
import com.facebook.presto.genericthrift.client.ThriftTableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnNotFoundException;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.genericthrift.client.ThriftConnectorSession.fromConnectorSession;
import static com.facebook.presto.genericthrift.client.ThriftSchemaTableName.fromSchemaTableName;
import static com.facebook.presto.genericthrift.client.ThriftTableLayoutResult.toConnectorTableLayoutResult;
import static com.facebook.presto.genericthrift.client.ThriftTableMetadata.toConnectorTableMetadata;
import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.fromTupleDomain;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class GenericThriftMetadata
        implements ConnectorMetadata
{
    private final PrestoClientProvider clientProvider;
    private final TypeManager typeManager;
    private final GenericThriftClientSessionProperties clientSessionProperties;

    @Inject
    public GenericThriftMetadata(PrestoClientProvider clientProvider, TypeManager typeManager, GenericThriftClientSessionProperties clientSessionProperties)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.clientSessionProperties = requireNonNull(clientSessionProperties, "clientSessionProperties is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            return client.listSchemaNames(fromConnectorSession(session, clientSessionProperties));
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            ThriftNullableTableMetadata thriftTableMetadata = client.getTableMetadata(fromConnectorSession(session, clientSessionProperties), fromSchemaTableName(tableName));
            if (thriftTableMetadata.getThriftTableMetadata() == null) {
                return null;
            }
            ThriftSchemaTableName schemaTableName = thriftTableMetadata.getThriftTableMetadata().getSchemaTableName();
            return new GenericThriftTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        GenericThriftTableHandle tableHandle = (GenericThriftTableHandle) table;
        List<ThriftTableLayoutResult> thriftLayoutResults;
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            thriftLayoutResults = client.getTableLayouts(
                    fromConnectorSession(session, clientSessionProperties),
                    new ThriftSchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()),
                    fromTupleDomain(constraint.getSummary()),
                    desiredColumns.map(GenericThriftMetadata::columnNames).orElse(null));
        }
        Map<String, ColumnHandle> allColumns = getColumnHandles(session, table);
        return thriftLayoutResults.stream()
                .map(result -> toConnectorTableLayoutResult(
                        result,
                        new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()),
                        allColumns))
                .collect(toList());
    }

    private static Set<String> columnNames(Set<ColumnHandle> columns)
    {
        return columns.stream()
                .map(columnHandle -> ((GenericThriftColumnHandle) columnHandle).getColumnName())
                .collect(toSet());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        GenericThriftTableLayoutHandle thriftHandle = (GenericThriftTableLayoutHandle) handle;
        return new ConnectorTableLayout(
                thriftHandle,
                thriftHandle.getOutputColumns(),
                thriftHandle.getPredicate(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        GenericThriftTableHandle handle = ((GenericThriftTableHandle) table);
        return getTableMetadata(session, new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            ThriftNullableTableMetadata thriftTableMetadata = client.getTableMetadata(fromConnectorSession(session, clientSessionProperties), fromSchemaTableName(schemaTableName));
            if (thriftTableMetadata.getThriftTableMetadata() == null) {
                throw new TableNotFoundException(schemaTableName);
            }
            return toConnectorTableMetadata(thriftTableMetadata.getThriftTableMetadata(), typeManager);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            return client.listTables(fromConnectorSession(session, clientSessionProperties), schemaNameOrNull)
                    .stream()
                    .map(thriftSchemaTable -> new SchemaTableName(thriftSchemaTable.getSchemaName(), thriftSchemaTable.getTableName()))
                    .collect(toList());
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> result = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            result.put(columnMetadata.getName(), new GenericThriftColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return result.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        GenericThriftColumnHandle requiredColumn = ((GenericThriftColumnHandle) columnHandle);
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        for (ColumnMetadata actualColumn : tableMetadata.getColumns()) {
            if (actualColumn.getName().equals(requiredColumn.getColumnName())) {
                return actualColumn;
            }
        }
        GenericThriftTableHandle table = (GenericThriftTableHandle) tableHandle;
        throw new ColumnNotFoundException(
                new SchemaTableName(table.getSchemaName(), table.getTableName()),
                requiredColumn.getColumnName());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }
}
