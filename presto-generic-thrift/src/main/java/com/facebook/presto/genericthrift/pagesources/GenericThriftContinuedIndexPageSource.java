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
package com.facebook.presto.genericthrift.pagesources;

import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.client.ThriftSplitsOrRows;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static java.util.Objects.requireNonNull;

public class GenericThriftContinuedIndexPageSource
        extends GenericThriftAbstractPageSource
{
    private final ThriftPrestoClient client;
    private final ThriftRowsBatch keys;
    private final byte[] indexId;

    public GenericThriftContinuedIndexPageSource(
            ThriftRowsBatch initialResponse,
            ThriftPrestoClient client,
            byte[] indexId,
            ThriftRowsBatch keys,
            List<ColumnHandle> columns)
    {
        super(columns, initialResponse);
        this.client = requireNonNull(client, "client is null");
        this.keys = requireNonNull(keys, "keys is null");
        this.indexId = requireNonNull(indexId, "indexId is null");
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, int maxRecords)
    {
        return transform(
                client.getRowsOrSplitsForIndex(indexId, keys, 0, maxRecords, nextToken),
                GenericThriftContinuedIndexPageSource::getRowsBatch);
    }

    @Override
    public void closeInternal()
    {
        client.close();
    }

    private static ThriftRowsBatch getRowsBatch(ThriftSplitsOrRows splitsOrRows)
    {
        requireNonNull(splitsOrRows, "splitsOrRows is null");
        checkState(splitsOrRows.getRows() != null, "rows must be present");
        return splitsOrRows.getRows();
    }
}
