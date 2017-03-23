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

import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * Presto Thrift service definition.
 * This thrift service needs to be implemented on the Thrift Node in order to be used with Thrift Connector.
 */
@ThriftService("presto-thrift")
public interface ThriftPrestoClient
        extends Closeable
{
    /**
     * Returns definitions for session properties supported by Thrift Node.
     * Session properties which are different from their default values will be passed to Thrift Node in selected method calls.
     * This method is called once on Thrift Connector start up.
     *
     * @return definitions for session properties supported by Thrift Node.
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<ThriftPropertyMetadata> listSessionProperties();

    /**
     * Returns available schema names.
     *
     * @return available schema names
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<String> listSchemaNames();

    /**
     * Returns tables for the given schema name.
     *
     * @param schemaNameOrNull schema name or {@literal null} value.
     * @return a list of table names with corresponding schemas. If schema name is null then returns
     * a list of tables for all schemas. Returns an empty list of a schema doesn't exist.
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<ThriftSchemaTableName> listTables(@Nullable String schemaNameOrNull);

    /**
     * Returns metadata for a given table.
     *
     * @param schemaTableName schema and table name
     * @return metadata for a given table, or a {@literal null} value inside if it doesn't exist
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ThriftNullableTableMetadata getTableMetadata(ThriftSchemaTableName schemaTableName);

    /**
     * Returns supported table layouts for a given table, desired columns and query constraint.
     *
     * @param session session properties
     * @param schemaTableName schema and table name
     * @param outputConstraint query constraint
     * @param desiredColumns a superset of columns used by the query.
     * {@literal null} value means "all columns" while empty set means "no columns".
     * @return supported table layouts
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    List<ThriftTableLayoutResult> getTableLayouts(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            ThriftTupleDomain outputConstraint,
            @Nullable Set<String> desiredColumns);

    /**
     * Returns a batch of splits.
     *
     * @param session session properties
     * @param layout table layout chosen by the engine
     * @param maxSplitCount maximum number of splits to return
     * @param nextToken token from a previous split batch or {@literal null} if it's the first call
     * @return a batch of splits
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftSplitBatch> getSplits(
            ThriftConnectorSession session,
            ThriftTableLayout layout,
            int maxSplitCount,
            @Nullable byte[] nextToken);

    /**
     * Returns a batch of rows for the given split.
     *
     * @param splitId split id as returned in split batch
     * @param maxBytes maximum size of returned data in bytes
     * @param nextToken token from a previous batch or {@literal null} if it's the first call
     * @return a batch of table data
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftRowsBatch> getRows(byte[] splitId, long maxBytes, @Nullable byte[] nextToken);

    /**
     * Returns index layout if a table can be used for index lookups with the given constraints.
     * The engine will make this call if there's an opportunity to use an index join.
     *
     * @param session session properties
     * @param schemaTableName schema and table name
     * @param indexableColumnNames column names used as a lookup key
     * @param outputColumnNames columns that need to be returned as a result of a lookup
     * @param outputConstraint constraint for the returned rows
     * @return index layout if a table can be used for index lookups or a structure containing {@literal null} otherwise
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ThriftNullableIndexLayoutResult resolveIndex(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            Set<String> indexableColumnNames,
            Set<String> outputColumnNames,
            ThriftTupleDomain outputConstraint);

    /**
     * Returns either a batch of index splits or a batch of data. Exactly one of the two must be present.
     * If this method returns a batch of splits then {@code getRows} will be called for each split
     * until split's {@code nextToken} is {@literal null}. Then the next call will be made to this method
     * to get the next batch of splits.
     * If this method returns a batch or rows then this method will continue to be called until {@code nextToken} is {@literal null}.
     * <p>
     * Having an opportunity to return either results or splits give implementer an opportunity to use the one best suited for one's goals.
     * Returning data helps to avoid extra round-trips, especially in cases where results would fit in one batch.
     * Returning splits allows to have control of what hosts are used to get data from
     * as well as have an ability to divide the work into units as desired.
     *
     * @param indexId index id as returned by {@code resolveIndex}
     * @param keys index key values
     * @param maxSplitCount maximum number of splits if this method returns splits
     * @param rowsMaxBytes maximum size of data if this method returns data
     * @param nextToken token from a previous call result or {@literal null} if it's the first call
     * @return either a batch of splits or a batch of data
     */
    @ThriftMethod(exception = @ThriftException(type = ThriftServiceException.class, id = 1))
    ListenableFuture<ThriftSplitsOrRows> getRowsOrSplitsForIndex(
            byte[] indexId,
            ThriftRowsBatch keys,
            int maxSplitCount,
            long rowsMaxBytes,
            @Nullable byte[] nextToken);

    @Override
    void close();
}
