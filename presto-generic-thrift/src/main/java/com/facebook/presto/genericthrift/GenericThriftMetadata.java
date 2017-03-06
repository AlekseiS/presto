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
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.genericthrift.client.ThriftConnectorSession.fromConnectorSession;
import static com.facebook.presto.genericthrift.client.ThriftSchemaTableName.fromSchemaTableName;
import static com.facebook.presto.genericthrift.client.ThriftTableLayoutResult.toConnectorTableLayoutResult;
import static com.facebook.presto.genericthrift.client.ThriftTableMetadata.toConnectorTableMetadata;
import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.fromTupleDomain;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class GenericThriftMetadata
        implements ConnectorMetadata
{
    private static final int NUMBER_OF_REFRESH_THREADS = 10;
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);
    private static final Duration REFRESH_AFTER_WRITE = new Duration(2, MINUTES);

    private final PrestoClientProvider clientProvider;
    private final GenericThriftClientSessionProperties clientSessionProperties;
    private final ExecutorService executor;
    private final LoadingCache<SchemaTableName, Optional<ConnectorTableMetadata>> tableCache;

    @Inject
    public GenericThriftMetadata(PrestoClientProvider clientProvider, TypeManager typeManager, GenericThriftClientSessionProperties clientSessionProperties)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        requireNonNull(typeManager, "typeManager is null");
        this.clientSessionProperties = requireNonNull(clientSessionProperties, "clientSessionProperties is null");
        this.executor = newFixedThreadPool(NUMBER_OF_REFRESH_THREADS, daemonThreadsNamed("metadata-refresh-%s"));
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
                }, executor));
    }

    @SuppressWarnings("unused")
    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
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
            return new GenericThriftTableHandle(actualTableName.getSchemaName(), actualTableName.getTableName());
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
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        GenericThriftTableHandle handle = ((GenericThriftTableHandle) tableHandle);
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
                    new GenericThriftColumnHandle(columnMetadata.getName(), columnMetadata.getType(), columnMetadata.getComment(), columnMetadata.isHidden()));
        }
        return result.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        GenericThriftColumnHandle handle = ((GenericThriftColumnHandle) columnHandle);
        return new ColumnMetadata(handle.getColumnName(), handle.getColumnType(), handle.getComment(), handle.isHidden());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }
}
