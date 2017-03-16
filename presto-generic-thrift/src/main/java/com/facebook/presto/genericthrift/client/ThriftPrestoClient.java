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

import com.facebook.swift.service.ThriftException;
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
     * Loaded once on Presto start up.
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<ThriftPropertyMetadata> listSessionProperties();

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<String> listSchemaNames();

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<ThriftSchemaTableName> listTables(@Nullable String schemaNameOrNull);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ThriftNullableTableMetadata getTableMetadata(ThriftSchemaTableName schemaTableName);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<ThriftTableLayoutResult> getTableLayouts(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            ThriftTupleDomain outputConstraint,
            @Nullable Set<String> desiredColumns);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftSplitBatch> getSplits(
            ThriftConnectorSession session,
            ThriftTableLayout layout,
            int maxSplitCount,
            @Nullable byte[] continuationToken);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftRowsBatch> getRows(byte[] splitId, int maxRowCount, @Nullable byte[] continuationToken);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ThriftNullableIndexLayoutResult resolveIndex(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            Set<String> indexableColumnNames,
            Set<String> outputColumnNames,
            ThriftTupleDomain outputConstraint);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ThriftSplitsOrRows getRowsOrSplitsForIndex(
            byte[] indexId,
            ThriftRowsBatch keys,
            int maxSplitCount,
            int maxRowCount);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftSplitBatch> getSplitsForIndexContinued(
            byte[] indexId,
            ThriftRowsBatch keys,
            int maxSplitCount,
            @Nullable byte[] continuationToken);

    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftRowsBatch> getRowsForIndexContinued(
            byte[] indexId,
            ThriftRowsBatch keys,
            int maxRowCount,
            @Nullable byte[] continuationToken);

    @Override
    void close();
}
