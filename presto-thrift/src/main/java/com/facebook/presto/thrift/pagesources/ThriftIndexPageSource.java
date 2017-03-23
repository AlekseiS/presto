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
package com.facebook.presto.thrift.pagesources;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.thrift.ThriftConfig;
import com.facebook.presto.thrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.thrift.interfaces.client.ThriftPrestoClient;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftSplit;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitsOrRows;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.thrift.interfaces.client.ThriftHostAddress.toHostAddressList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.util.Objects.requireNonNull;

public class ThriftIndexPageSource
        extends ThriftAbstractPageSource
{
    private final PrestoClientProvider clientProvider;
    private final ThriftRowsBatch keys;
    private final byte[] indexId;
    private final int maxSplitsPerBatch;

    // modified from other threads
    private AtomicReference<ThriftPrestoClient> client;
    private AtomicReference<State> state;

    public ThriftIndexPageSource(
            PrestoClientProvider clientProvider,
            byte[] indexId,
            ThriftRowsBatch keys,
            List<ColumnHandle> columns,
            ThriftConfig config)
    {
        super(columns, config);
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.keys = requireNonNull(keys, "keys is null");
        this.indexId = requireNonNull(indexId, "indexId is null");
        this.maxSplitsPerBatch = requireNonNull(config, "config is null").getMaxIndexSplitsPerBatch();
        this.client = new AtomicReference<>(clientProvider.connectToAnyHost());
        this.state = new AtomicReference<>(new InitialState());
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, long maxBytes)
    {
        return state.get().sendRequestForData(nextToken, maxBytes);
    }

    @Override
    public boolean canGetMoreData(byte[] nextToken)
    {
        return state.get().canGetMoreData(nextToken);
    }

    @Override
    public void closeInternal()
    {
        client.get().close();
    }

    private interface State
    {
        ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, long maxBytes);

        boolean canGetMoreData(byte[] nextToken);
    }

    private final class InitialState
            implements State
    {
        @Override
        public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, long maxBytes)
        {
            checkState(nextToken == null, "first call must pass null for nextToken");
            return transformAsync(
                    client.get().getRowsOrSplitsForIndex(indexId, keys, 0, maxBytes, nextToken),
                    splitsOrRows -> getSplitsOrRows(splitsOrRows, maxBytes));
        }

        @Override
        public boolean canGetMoreData(byte[] nextToken)
        {
            return true;
        }

        private ListenableFuture<ThriftRowsBatch> getSplitsOrRows(ThriftSplitsOrRows splitsOrRows, long maxBytes)
        {
            if (splitsOrRows.getSplits() != null) {
                State newState = new SplitBasedState(splitsOrRows.getSplits());
                state.set(newState);
                return newState.sendRequestForData(null, maxBytes);
            }
            else if (splitsOrRows.getRows() != null) {
                state.set(new RowBasedState());
                return immediateFuture(splitsOrRows.getRows());
            }
            else {
                throw new IllegalStateException("Unknown state of splits or rows data structure");
            }
        }
    }

    private final class RowBasedState
            implements State
    {
        @Override
        public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, long maxBytes)
        {
            return transform(client.get().getRowsOrSplitsForIndex(indexId, keys, 0, maxBytes, nextToken), this::getRows);
        }

        @Override
        public boolean canGetMoreData(byte[] nextToken)
        {
            return nextToken != null;
        }

        private ThriftRowsBatch getRows(ThriftSplitsOrRows splitsOrRows)
        {
            checkState(splitsOrRows.getRows() != null, "rows must be present");
            return splitsOrRows.getRows();
        }
    }

    private final class SplitBasedState
            implements State
    {
        private final AtomicReference<Iterator<ThriftSplit>> splitIterator;
        private final AtomicReference<ThriftSplit> currentSplit = new AtomicReference<>(null);
        private final AtomicReference<byte[]> splitsContinuationToken;

        public SplitBasedState(ThriftSplitBatch splits)
        {
            splitIterator = new AtomicReference<>(splits.getSplits().iterator());
            splitsContinuationToken = new AtomicReference<>(splits.getNextToken());
        }

        @Override
        public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, long maxBytes)
        {
            if (nextToken != null) {
                // current split still has data
                return client.get().getRows(currentSplit.get().getSplitId(), maxBytes, nextToken);
            }
            else if (splitIterator.get().hasNext()) {
                // current split is finished, get a new one
                return startNextSplit(maxBytes);
            }
            else {
                // current split batch is empty
                if (splitsContinuationToken.get() != null) {
                    // send request for a new split batch
                    return transformAsync(
                            client.get().getRowsOrSplitsForIndex(indexId, keys, maxSplitsPerBatch, 0, splitsContinuationToken.get()),
                            splitsOrRows -> processSplitBatch(splitsOrRows, maxBytes));
                }
                else {
                    // all splits were processed
                    // shouldn't get here if checks work properly
                    throw new IllegalStateException("All splits were processed, but got request for more data");
                }
            }
        }

        private ListenableFuture<ThriftRowsBatch> processSplitBatch(ThriftSplitsOrRows splitsOrRows, long maxBytes)
        {
            checkState(splitsOrRows.getSplits() != null, "splits must be present");
            ThriftSplitBatch splitBatch = splitsOrRows.getSplits();
            splitIterator.set(splitBatch.getSplits().iterator());
            splitsContinuationToken.set(splitBatch.getNextToken());
            if (splitBatch.getSplits().isEmpty()) {
                return immediateFuture(ThriftRowsBatch.empty());
            }
            else {
                return startNextSplit(maxBytes);
            }
        }

        private ListenableFuture<ThriftRowsBatch> startNextSplit(long maxBytes)
        {
            ThriftSplit split = splitIterator.get().next();
            currentSplit.set(split);
            if (!split.getHosts().isEmpty()) {
                // reconnect to a new host
                client.get().close();
                client.set(clientProvider.connectToAnyOf(toHostAddressList(split.getHosts())));
            }
            return client.get().getRows(split.getSplitId(), maxBytes, null);
        }

        @Override
        public boolean canGetMoreData(byte[] nextToken)
        {
            return nextToken != null || splitIterator.get().hasNext() || splitsContinuationToken.get() != null;
        }
    }
}
