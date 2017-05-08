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

import com.facebook.presto.connector.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.connector.thrift.api.PrestoThriftDomain;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableIndexLayoutResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.facebook.presto.connector.thrift.ThriftColumnHandle.tupleDomainToThriftTupleDomain;
import static com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName.fromSchemaTableName;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ThriftMetadata
        implements ConnectorMetadata
{
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);
    private static final Duration REFRESH_AFTER_WRITE = new Duration(2, MINUTES);

    private final PrestoThriftServiceProvider clientProvider;
    private final ThriftClientSessionProperties clientSessionProperties;
    private final LoadingCache<SchemaTableName, Optional<ConnectorTableMetadata>> tableCache;

    @Inject
    public ThriftMetadata(
            PrestoThriftServiceProvider clientProvider,
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
                        return clientProvider.runOnAnyHost(client -> {
                            PrestoThriftNullableTableMetadata thriftTableMetadata = client.getTableMetadata(fromSchemaTableName(schemaTableName));
                            if (thriftTableMetadata.getThriftTableMetadata() == null) {
                                return Optional.empty();
                            }
                            else {
                                ConnectorTableMetadata tableMetadata = thriftTableMetadata.getThriftTableMetadata().toConnectorTableMetadata(typeManager);
                                checkState(Objects.equals(schemaTableName, tableMetadata.getTable()), "requested and actual table names are different");
                                return Optional.of(tableMetadata);
                            }
                        });
                    }
                }, metadataRefreshExecutor));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return clientProvider.runOnAnyHost(PrestoThriftService::listSchemaNames);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ConnectorTableMetadata> tableMetadata = tableCache.getUnchecked(tableName);
        if (!tableMetadata.isPresent()) {
            return null;
        }
        else {
            SchemaTableName storedTableName = tableMetadata.get().getTable();
            return new ThriftTableHandle(storedTableName.getSchemaName(), storedTableName.getTableName());
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
        ThriftTableLayoutHandle layoutHandle = new ThriftTableLayoutHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                desiredColumns,
                constraint.getSummary());
        return ImmutableList.of(new ConnectorTableLayoutResult(new ConnectorTableLayout(layoutHandle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
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
        return clientProvider.runOnAnyHost(client -> client.listTables(new PrestoThriftNullableSchemaName(schemaNameOrNull)))
                .stream()
                .map(thriftSchemaTable -> new SchemaTableName(thriftSchemaTable.getSchemaName(), thriftSchemaTable.getTableName()))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, tableHandle).getColumns()
                .stream()
                .collect(toImmutableMap(
                        ColumnMetadata::getName,
                        columnMetadata -> new ThriftColumnHandle(
                                columnMetadata.getName(),
                                columnMetadata.getType(),
                                columnMetadata.getComment(),
                                columnMetadata.isHidden())));
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
        PrestoThriftNullableIndexLayoutResult result = clientProvider.runOnAnyHost(
                client -> client.resolveIndex(ThriftClientSessionProperties.toThriftSession(session, clientSessionProperties),
                        new PrestoThriftSchemaTableName(thriftTableHandle.getSchemaName(), thriftTableHandle.getTableName()),
                        ThriftColumnHandle.columnNames(indexableColumns),
                        ThriftColumnHandle.columnNames(outputColumns),
                        tupleDomainToThriftTupleDomain(tupleDomain)));
        if (result.getIndexLayoutResult() == null) {
            return Optional.empty();
        }
        else {
            return Optional.of(new ConnectorResolvedIndex(
                    new ThriftIndexHandle(result.getIndexLayoutResult().getIndexId()),
                    toTupleDomain(result.getIndexLayoutResult().getUnenforcedPredicate(), getColumnHandles(session, tableHandle))));
        }
    }

    private static TupleDomain<ColumnHandle> toTupleDomain(
            PrestoThriftTupleDomain thriftTupleDomain,
            Map<String, ColumnHandle> allColumns)
    {
        if (thriftTupleDomain.getDomains() == null) {
            return TupleDomain.none();
        }
        Map<ColumnHandle, Domain> tupleDomains = new HashMap<>(thriftTupleDomain.getDomains().size());
        for (Map.Entry<String, PrestoThriftDomain> kv : thriftTupleDomain.getDomains().entrySet()) {
            ThriftColumnHandle handle = (ThriftColumnHandle) requireNonNull(allColumns.get(kv.getKey()),
                    "Column handle is not present");
            Domain domain = kv.getValue().toDomain(handle.getColumnType());
            tupleDomains.put(handle, domain);
        }
        return TupleDomain.withColumnDomains(tupleDomains);
    }
}
