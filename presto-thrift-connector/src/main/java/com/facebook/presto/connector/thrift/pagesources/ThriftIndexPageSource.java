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
package com.facebook.presto.connector.thrift.pagesources;

import com.facebook.presto.connector.thrift.ThriftConnectorConfig;
import com.facebook.presto.connector.thrift.api.PrestoThriftRowsBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitsOrRows;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.connector.thrift.api.PrestoThriftHostAddress.toHostAddressList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.util.Objects.requireNonNull;

public class ThriftIndexPageSource
        extends AbstractThriftPageSource
{
    private final PrestoThriftServiceProvider clientProvider;
    private final PrestoThriftRowsBatch keys;
    private final byte[] indexId;
    private final int maxSplitsPerBatch;

    // modified from other threads
    private AtomicReference<PrestoThriftService> client;
    private AtomicReference<State> state;

    public ThriftIndexPageSource(
            PrestoThriftServiceProvider clientProvider,
            byte[] indexId,
            PrestoThriftRowsBatch keys,
            List<ColumnHandle> columns,
            ThriftConnectorConfig config)
    {
        super(columns, config);
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.keys = requireNonNull(keys, "keys is null");
        this.indexId = requireNonNull(indexId, "indexId is null");
        this.maxSplitsPerBatch = requireNonNull(config, "config is null").getMaxIndexSplitsPerBatch();
        this.client = new AtomicReference<>(clientProvider.anyHostClient());
        this.state = new AtomicReference<>(new InitialState());
    }

    @Override
    public ListenableFuture<PrestoThriftRowsBatch> sendDataRequest(byte[] nextToken)
    {
        return state.get().sendRequestForData(nextToken);
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
        ListenableFuture<PrestoThriftRowsBatch> sendRequestForData(byte[] nextToken);

        boolean canGetMoreData(byte[] nextToken);
    }

    private final class InitialState
            implements State
    {
        @Override
        public ListenableFuture<PrestoThriftRowsBatch> sendRequestForData(byte[] nextToken)
        {
            checkState(nextToken == null, "first call must pass null for nextToken");
            return transformAsync(
                    client.get().getRowsOrSplitsForIndex(indexId, keys, 0, getMaxBytesPerResponse(), nextToken),
                    this::getSplitsOrRows);
        }

        @Override
        public boolean canGetMoreData(byte[] nextToken)
        {
            return true;
        }

        private ListenableFuture<PrestoThriftRowsBatch> getSplitsOrRows(PrestoThriftSplitsOrRows splitsOrRows)
        {
            if (splitsOrRows.getSplits() != null) {
                State newState = new SplitBasedState(splitsOrRows.getSplits());
                state.set(newState);
                return newState.sendRequestForData(null);
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
        public ListenableFuture<PrestoThriftRowsBatch> sendRequestForData(byte[] nextToken)
        {
            return transform(client.get().getRowsOrSplitsForIndex(indexId, keys, 0, getMaxBytesPerResponse(), nextToken), this::getRows);
        }

        @Override
        public boolean canGetMoreData(byte[] nextToken)
        {
            return nextToken != null;
        }

        private PrestoThriftRowsBatch getRows(PrestoThriftSplitsOrRows splitsOrRows)
        {
            checkState(splitsOrRows.getRows() != null, "rows must be present");
            return splitsOrRows.getRows();
        }
    }

    private final class SplitBasedState
            implements State
    {
        private final AtomicReference<Iterator<PrestoThriftSplit>> splitIterator;
        private final AtomicReference<PrestoThriftSplit> currentSplit = new AtomicReference<>(null);
        private final AtomicReference<byte[]> splitsNextToken;

        public SplitBasedState(PrestoThriftSplitBatch splits)
        {
            splitIterator = new AtomicReference<>(splits.getSplits().iterator());
            splitsNextToken = new AtomicReference<>(splits.getNextToken());
        }

        @Override
        public ListenableFuture<PrestoThriftRowsBatch> sendRequestForData(byte[] nextToken)
        {
            if (nextToken != null) {
                // current split still has data
                return client.get().getRows(currentSplit.get().getSplitId(), getColumnNames(), getMaxBytesPerResponse(), nextToken);
            }
            else if (splitIterator.get().hasNext()) {
                // current split is finished, get a new one
                return startNextSplit();
            }
            else {
                // current split batch is empty
                if (splitsNextToken.get() != null) {
                    // send request for a new split batch
                    return transformAsync(
                            client.get().getRowsOrSplitsForIndex(indexId, keys, maxSplitsPerBatch, 0, splitsNextToken.get()),
                            this::processSplitBatch);
                }
                else {
                    // all splits were processed
                    // shouldn't get here if checks work properly
                    throw new IllegalStateException("All splits were processed, but got request for more data");
                }
            }
        }

        private ListenableFuture<PrestoThriftRowsBatch> processSplitBatch(PrestoThriftSplitsOrRows splitsOrRows)
        {
            checkState(splitsOrRows.getSplits() != null, "splits must be present");
            PrestoThriftSplitBatch splitBatch = splitsOrRows.getSplits();
            splitIterator.set(splitBatch.getSplits().iterator());
            splitsNextToken.set(splitBatch.getNextToken());
            if (splitBatch.getSplits().isEmpty()) {
                return immediateFuture(emptyBatch());
            }
            else {
                return startNextSplit();
            }
        }

        private ListenableFuture<PrestoThriftRowsBatch> startNextSplit()
        {
            PrestoThriftSplit split = splitIterator.get().next();
            currentSplit.set(split);
            if (!split.getHosts().isEmpty()) {
                // reconnect to a new host
                client.get().close();
                client.set(clientProvider.selectedHostClient(toHostAddressList(split.getHosts())));
            }
            return client.get().getRows(split.getSplitId(), getColumnNames(), getMaxBytesPerResponse(), null);
        }

        @Override
        public boolean canGetMoreData(byte[] nextToken)
        {
            return nextToken != null || splitIterator.get().hasNext() || splitsNextToken.get() != null;
        }

        private PrestoThriftRowsBatch emptyBatch()
        {
            return new PrestoThriftRowsBatch(ImmutableList.of(), 0, null);
        }
    }
}
