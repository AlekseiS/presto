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

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

@ThriftService("presto-generic-thrift")
public interface ThriftPrestoClient
        extends Closeable
{
    /**
     * Loaded exactly once on Presto start up
     */
    @ThriftMethod
    List<ThriftPropertyMetadata> listSessionProperties();

    @ThriftMethod
    List<String> listSchemaNames(ThriftConnectorSession session);

    @ThriftMethod
    List<ThriftSchemaTableName> listTables(ThriftConnectorSession session, @Nullable String schemaNameOrNull);

    @ThriftMethod
    ThriftNullableTableMetadata getTableMetadata(ThriftConnectorSession session, ThriftSchemaTableName schemaTableName);

    @ThriftMethod
    List<ThriftTableLayoutResult> getTableLayouts(ThriftConnectorSession session, ThriftSchemaTableName schemaTableName, ThriftTupleDomain outputConstraint, @Nullable Set<String> desiredColumns);

    @ThriftMethod
    ListenableFuture<ThriftSplitBatch> getSplitBatch(ThriftConnectorSession session, ThriftSchemaTableName schemaTableName, ThriftTableLayout layout, int maxSplitCount, @Nullable String continuationToken);

    @ThriftMethod
    ThriftRowsBatch getRows(String splitId, List<String> columnNames, int maxRowCount, @Nullable String continuationToken);

    @Override
    void close();
}
