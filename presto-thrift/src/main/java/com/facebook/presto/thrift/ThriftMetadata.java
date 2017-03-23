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
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorResolvedIndex;
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
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.thrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.thrift.interfaces.client.ThriftNullableIndexLayoutResult;
import com.facebook.presto.thrift.interfaces.client.ThriftNullableTableMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftPrestoClient;
import com.facebook.presto.thrift.interfaces.client.ThriftSchemaTableName;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayoutResult;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.facebook.presto.thrift.ThriftClientSessionProperties.toThriftSession;
import static com.facebook.presto.thrift.ThriftColumnHandle.thriftTupleDomainToTupleDomain;
import static com.facebook.presto.thrift.ThriftColumnHandle.tupleDomainToThriftTupleDomain;
import static com.facebook.presto.thrift.ThriftTableLayoutHandle.thriftLayoutResultToTableLayoutResult;
import static com.facebook.presto.thrift.interfaces.client.ThriftSchemaTableName.fromSchemaTableName;
import static com.facebook.presto.thrift.interfaces.client.ThriftTableMetadata.toConnectorTableMetadata;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ThriftMetadata
        implements ConnectorMetadata
{
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);
    private static final Duration REFRESH_AFTER_WRITE = new Duration(2, MINUTES);

    private final PrestoClientProvider clientProvider;
    private final ThriftClientSessionProperties clientSessionProperties;
    private final LoadingCache<SchemaTableName, Optional<ConnectorTableMetadata>> tableCache;

    @Inject
    public ThriftMetadata(
            PrestoClientProvider clientProvider,
            TypeManager typeManager,
            ThriftClientSessionProperties clientSessionProperties,
            @ForMetadataRefresh Executor metadataRefreshExecutor)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        requireNonNull(typeManager, "typeManager is null");
        this.clientSessionProperties = requireNonNull(clientSessionProperties, "clientSessionProperties is null");
        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(EXPIRE_AFTER_WRITE.toMillis(), MILLISECONDS)
                .refreshAfterWrite(REFRESH_AFTER_WRITE.toMillis(), MILLISECONDS)
                .build(asyncReloading(new CacheLoader<SchemaTableName, Optional<ConnectorTableMetadata>>()
                {
                    @Override
                    public Optional<ConnectorTableMetadata> load(SchemaTableName schemaTableName)
                            throws Exception
                    {
                        requireNonNull(schemaTableName, "schemaTableName is null");
                        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
                            ThriftNullableTableMetadata thriftTableMetadata = client.getTableMetadata(fromSchemaTableName(schemaTableName));
                            if (thriftTableMetadata.getThriftTableMetadata() == null) {
                                return Optional.empty();
                            }
                            else {
                                return Optional.of(toConnectorTableMetadata(thriftTableMetadata.getThriftTableMetadata(), typeManager));
                            }
                        }
                    }
                }, metadataRefreshExecutor));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            return client.listSchemaNames();
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ConnectorTableMetadata> tableMetadata = tableCache.getUnchecked(tableName);
        if (!tableMetadata.isPresent()) {
            return null;
        }
        else {
            SchemaTableName actualTableName = tableMetadata.get().getTable();
            return new ThriftTableHandle(actualTableName.getSchemaName(), actualTableName.getTableName());
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        ThriftTableHandle tableHandle = (ThriftTableHandle) table;
        Optional<Set<String>> desiredColumnNames = desiredColumns.map(ThriftMetadata::columnNames);
        List<ThriftTableLayoutResult> thriftLayoutResults;
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            thriftLayoutResults = client.getTableLayouts(
                    toThriftSession(session, clientSessionProperties),
                    new ThriftSchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()),
                    tupleDomainToThriftTupleDomain(constraint.getSummary()),
                    desiredColumnNames.orElse(null));
        }
        Map<String, ColumnHandle> allColumns = getColumnHandles(session, table);
        return thriftLayoutResults.stream()
                .map(result -> thriftLayoutResultToTableLayoutResult(
                        result,
                        allColumns))
                .collect(toList());
    }

    private static Set<String> columnNames(Set<ColumnHandle> columns)
    {
        return columns.stream()
                .map(columnHandle -> ((ThriftColumnHandle) columnHandle).getColumnName())
                .collect(toSet());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        ThriftTableLayoutHandle thriftHandle = (ThriftTableLayoutHandle) handle;
        return new ConnectorTableLayout(
                thriftHandle,
                Optional.empty(),
                thriftHandle.getPredicate(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ThriftTableHandle handle = ((ThriftTableHandle) tableHandle);
        return getTableMetadata(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        Optional<ConnectorTableMetadata> table = tableCache.getUnchecked(schemaTableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(schemaTableName);
        }
        else {
            return table.get();
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            return client.listTables(schemaNameOrNull)
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
            result.put(columnMetadata.getName(),
                    new ThriftColumnHandle(columnMetadata.getName(), columnMetadata.getType(), columnMetadata.getComment(), columnMetadata.isHidden()));
        }
        return result.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ThriftColumnHandle handle = ((ThriftColumnHandle) columnHandle);
        return new ColumnMetadata(handle.getColumnName(), handle.getColumnType(), handle.getComment(), handle.isHidden());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        ThriftTableHandle thriftTableHandle = (ThriftTableHandle) tableHandle;
        try (ThriftPrestoClient client = clientProvider.connectToAnyHost()) {
            ThriftNullableIndexLayoutResult result = client.resolveIndex(toThriftSession(session, clientSessionProperties),
                    new ThriftSchemaTableName(thriftTableHandle.getSchemaName(), thriftTableHandle.getTableName()),
                    indexableColumns.stream().map(handle -> ((ThriftColumnHandle) handle).getColumnName()).collect(toSet()),
                    outputColumns.stream().map(handle -> ((ThriftColumnHandle) handle).getColumnName()).collect(toSet()),
                    tupleDomainToThriftTupleDomain(tupleDomain));
            if (result.getIndexLayoutResult() == null) {
                return Optional.empty();
            }
            else {
                return Optional.of(new ConnectorResolvedIndex(
                        new ThriftIndexHandle(result.getIndexLayoutResult().getIndexId()),
                        thriftTupleDomainToTupleDomain(result.getIndexLayoutResult().getUnenforcedPredicate(), getColumnHandles(session, tableHandle))));
            }
        }
    }
}
