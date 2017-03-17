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
import com.facebook.presto.genericthrift.client.ThriftSplit;
import com.facebook.presto.genericthrift.client.ThriftSplitBatch;
import com.facebook.presto.genericthrift.client.ThriftSplitsOrRows;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.genericthrift.client.ThriftHostAddress.toHostAddressList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.util.Objects.requireNonNull;

public class GenericThriftSplitBasedIndexPageSource
        extends GenericThriftAbstractPageSource
{
    private final PrestoClientProvider clientProvider;
    private final ThriftRowsBatch keys;
    private final byte[] indexId;
    private final int maxSplitsPerBatch;

    private final AtomicReference<ThriftPrestoClient> client;
    private final AtomicReference<Iterator<ThriftSplit>> splitIterator;
    private final AtomicReference<ThriftSplit> currentSplit = new AtomicReference<>(null);
    private final AtomicReference<byte[]> splitsContinuationToken;

    public GenericThriftSplitBasedIndexPageSource(
            ThriftSplitBatch splitBatch,
            PrestoClientProvider clientProvider,
            ThriftPrestoClient client,
            byte[] indexId,
            ThriftRowsBatch keys,
            List<ColumnHandle> outputColumns,
            int maxSplitsPerBatch)
    {
        super(outputColumns);
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.client = new AtomicReference<>(requireNonNull(client, "client is null"));
        this.keys = requireNonNull(keys, "keys is null");
        this.indexId = requireNonNull(indexId, "indexId is null");
        requireNonNull(splitBatch, "splitBatch is null");
        this.splitIterator = new AtomicReference<>(splitBatch.getSplits().iterator());
        this.splitsContinuationToken = new AtomicReference<>(splitBatch.getNextToken());
        checkArgument(maxSplitsPerBatch > 0, "maxSplitsPerBatch is zero or negative");
        this.maxSplitsPerBatch = maxSplitsPerBatch;
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, int maxRecords)
    {
        if (nextToken != null) {
            // current split still has data
            return client.get().getRows(currentSplit.get().getSplitId(), maxRecords, nextToken);
        }
        else if (splitIterator.get().hasNext()) {
            // current split is finished, get a new one
            return startNextSplit(maxRecords);
        }
        else {
            // current split batch is empty
            if (splitsContinuationToken.get() != null) {
                // send request for a new split batch
                ListenableFuture<ThriftSplitsOrRows> splitBatchFuture =
                        client.get().getRowsOrSplitsForIndex(indexId, keys, maxSplitsPerBatch, 0, splitsContinuationToken.get());
                return transformAsync(splitBatchFuture, splitsOrRows -> {
                    // received response with splits
                    ThriftSplitBatch splitBatch = getSplitBatch(splitsOrRows);
                    splitIterator.set(splitBatch.getSplits().iterator());
                    splitsContinuationToken.set(splitBatch.getNextToken());
                    checkState(splitBatch.getNextToken() == null || !splitBatch.getSplits().isEmpty(),
                            "Split batch cannot be empty when continuation token is present");
                    if (splitBatch.getSplits().isEmpty()) {
                        return immediateFuture(ThriftRowsBatch.empty());
                    }
                    else {
                        return startNextSplit(maxRecords);
                    }
                });
            }
            else {
                // all splits were processed
                // shouldn't get here if checks work properly
                throw new IllegalStateException("All splits were processed, but got request for more data");
            }
        }
    }

    @Override
    public boolean canGetMoreData(byte[] nextToken)
    {
        return nextToken != null || splitIterator.get().hasNext() || splitsContinuationToken.get() != null;
    }

    private static ThriftSplitBatch getSplitBatch(ThriftSplitsOrRows splitsOrRows)
    {
        requireNonNull(splitsOrRows, "splitsOrRows is null");
        checkState(splitsOrRows.getSplits() != null, "splits must be present");
        return splitsOrRows.getSplits();
    }

    private ListenableFuture<ThriftRowsBatch> startNextSplit(int maxRecords)
    {
        ThriftSplit split = splitIterator.get().next();
        currentSplit.set(split);
        if (!split.getHosts().isEmpty()) {
            // reconnect to a new host
            client.get().close();
            client.set(clientProvider.connectToAnyOf(toHostAddressList(split.getHosts())));
        }
        return client.get().getRows(split.getSplitId(), maxRecords, null);
    }

    @Override
    public void closeInternal()
    {
        client.get().close();
    }
}
