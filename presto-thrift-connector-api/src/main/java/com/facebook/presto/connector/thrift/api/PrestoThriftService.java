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

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * Presto Thrift service definition.
 * This thrift service needs to be implemented in order to be used with Thrift Connector.
 */
@ThriftService
public interface PrestoThriftService
        extends Closeable
{
    /**
     * Returns available schema names.
     */
    @ThriftMethod("prestoListSchemaNames")
    List<String> listSchemaNames()
            throws PrestoThriftServiceException;

    /**
     * Returns tables for the given schema name.
     *
     * @param schemaNameOrNull a structure containing schema name or {@literal null}
     * @return a list of table names with corresponding schemas. If schema name is null then returns
     * a list of tables for all schemas. Returns an empty list if a schema does not exist
     */
    @ThriftMethod("prestoListTables")
    List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
            throws PrestoThriftServiceException;

    /**
     * Returns metadata for a given table.
     *
     * @param schemaTableName schema and table name
     * @return metadata for a given table, or a {@literal null} value inside if it does not exist
     */
    @ThriftMethod("prestoGetTableMetadata")
    PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
            throws PrestoThriftServiceException;

    /**
     * Returns a batch of splits.
     *
     * @param schemaTableName schema and table name
     * @param desiredColumns a superset of columns to return; empty set means "no columns", {@literal null} set means "all columns"
     * @param outputConstraint constraint on the returned data
     * @param maxSplitCount maximum number of splits to return
     * @param nextToken token from a previous split batch or {@literal null} if it is the first call
     * @return a batch of splits
     */
    @ThriftMethod("prestoGetSplits")
    ListenableFuture<PrestoThriftSplitBatch> getSplits(
            PrestoThriftSchemaTableName schemaTableName,
            @Nullable Set<String> desiredColumns,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            @Nullable byte[] nextToken)
            throws PrestoThriftServiceException;

    /**
     * Returns a batch of rows for the given split.
     *
     * @param splitId split id as returned in split batch
     * @param columns a list of column names to return
     * @param maxBytes maximum size of returned data in bytes
     * @param nextToken token from a previous batch or {@literal null} if it is the first call
     * @return a batch of table data
     */
    @ThriftMethod("prestoGetRows")
    ListenableFuture<PrestoThriftRowsBatch> getRows(
            byte[] splitId,
            List<String> columns,
            long maxBytes,
            @Nullable byte[] nextToken)
            throws PrestoThriftServiceException;

    /**
     * Returns index layout if a table can be used for index lookups with the given constraints.
     * The engine will make this call if there is an opportunity to use an index join.
     *
     * @param schemaTableName schema and table name
     * @param indexableColumnNames column names used as a lookup key
     * @param outputColumnNames columns that need to be returned as a result of a lookup
     * @param outputConstraint constraint for the returned rows
     * @return index layout if a table can be used for index lookups or a structure containing {@literal null} otherwise
     */
    @ThriftMethod("prestoResolveIndex")
    PrestoThriftNullableIndexLayoutResult resolveIndex(
            PrestoThriftSchemaTableName schemaTableName,
            Set<String> indexableColumnNames,
            Set<String> outputColumnNames,
            PrestoThriftTupleDomain outputConstraint)
            throws PrestoThriftServiceException;

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
     * @param nextToken token from a previous call result or {@literal null} if it is the first call
     * @return either a batch of splits or a batch of data
     */
    @ThriftMethod("prestoGetRowsOrSplitsForIndex")
    ListenableFuture<PrestoThriftSplitsOrRows> getRowsOrSplitsForIndex(
            byte[] indexId,
            PrestoThriftRowsBatch keys,
            int maxSplitCount,
            long rowsMaxBytes,
            @Nullable byte[] nextToken)
            throws PrestoThriftServiceException;

    @Override
    void close();
}
