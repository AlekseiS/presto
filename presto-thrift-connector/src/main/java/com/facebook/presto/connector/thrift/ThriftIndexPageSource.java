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

import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.facebook.presto.connector.thrift.api.PrestoThriftHostAddress.toHostAddressList;
import static com.facebook.presto.connector.thrift.api.PrestoThriftPageResult.fromRecordSet;
import static com.facebook.presto.connector.thrift.util.TupleDomainConversion.tupleDomainToThriftTupleDomain;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class ThriftIndexPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(ThriftIndexPageSource.class);
    private static final int MAX_SPLIT_COUNT = 10_000_000;
    private static final int CONCURRENT_SPLITS = 2;
    // TODO: read from config
    private static final long MAX_BYTES = 16_000_000;

    private final PrestoThriftServiceProvider clientProvider;
    private final PrestoThriftSchemaTableName schemaTableName;
    private final List<String> lookupColumnNames;
    private final List<String> outputColumnNames;
    private final List<Type> outputColumnTypes;
    private final PrestoThriftPageResult keys;
    private final PrestoThriftTupleDomain outputConstraint;

    private CompletableFuture<?> statusFuture;
    private ListenableFuture<PrestoThriftSplitBatch> splitFuture;
    private ListenableFuture<PrestoThriftPageResult> dataSignalFuture;

    private final List<PrestoThriftSplit> splits = new ArrayList<>();
    private int splitIndex;
    private boolean haveSplits;
    private PrestoThriftService splitsClient;
    private int lastFoundPosition = -1;

    private final List<ListenableFuture<PrestoThriftPageResult>> dataRequests = new ArrayList<>(CONCURRENT_SPLITS);
    private final List<RunningSplitContext> contexts = new ArrayList<>(CONCURRENT_SPLITS);

    private boolean finished;

    public ThriftIndexPageSource(
            PrestoThriftServiceProvider clientProvider,
            ThriftIndexHandle indexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns,
            RecordSet keys)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");

        requireNonNull(indexHandle, "indexHandle is null");
        this.schemaTableName = new PrestoThriftSchemaTableName(indexHandle.getSchemaTableName());
        this.outputConstraint = tupleDomainToThriftTupleDomain(indexHandle.getTupleDomain());

        requireNonNull(lookupColumns, "lookupColumns is null");
        this.lookupColumnNames = lookupColumns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableList());

        requireNonNull(outputColumns, "outputColumns is null");
        ImmutableList.Builder<String> outputColumnNames = new ImmutableList.Builder<>();
        ImmutableList.Builder<Type> outputColumnTypes = new ImmutableList.Builder<>();
        for (ColumnHandle columnHandle : outputColumns) {
            ThriftColumnHandle thriftColumnHandle = (ThriftColumnHandle) columnHandle;
            outputColumnNames.add(thriftColumnHandle.getColumnName());
            outputColumnTypes.add(thriftColumnHandle.getColumnType());
        }
        this.outputColumnNames = outputColumnNames.build();
        this.outputColumnTypes = outputColumnTypes.build();

        this.keys = fromRecordSet(requireNonNull(keys, "keys is null"));
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        if (!haveSplits) {
            // check if request for splits was sent
            if (splitFuture == null) {
                // didn't start fetching splits, send the first request now
                splitsClient = clientProvider.anyHostClient();
                splitFuture = sendSplitRequest(splitsClient, null);
                statusFuture = toCompletableFuture(splitFuture);
            }
            if (!splitFuture.isDone()) {
                // split request is in progress
                return null;
            }
            // split request is ready
            PrestoThriftSplitBatch batch = getFutureValue(splitFuture);
            splits.addAll(batch.getSplits());
            // check if it's possible to request more splits
            if (batch.getNextToken() != null) {
                // can get more splits, send request
                splitFuture = sendSplitRequest(splitsClient, batch.getNextToken());
                statusFuture = toCompletableFuture(splitFuture);
                return null;
            }
            else {
                // no more splits
                splitFuture = null;
                statusFuture = null;
                haveSplits = true;
                splitsClient.close();
                splitsClient = null;
            }
        }
        checkState(haveSplits, "must have splits at this point");

        // check if any data requests were started
        if (dataSignalFuture == null) {
            // no data requests were started, start a number of initial requests
            checkState(contexts.isEmpty() && dataRequests.isEmpty(), "must have no running splits at this point");
            if (splits.isEmpty()) {
                // all done: no splits
                finished = true;
                return null;
            }
            for (int i = 0; i < min(CONCURRENT_SPLITS, splits.size()); i++) {
                PrestoThriftSplit split = splits.get(splitIndex);
                splitIndex++;
                RunningSplitContext context = new RunningSplitContext(openClient(split), split);
                contexts.add(context);
                dataRequests.add(sendDataRequest(context, null));
            }
            dataSignalFuture = whenAnyComplete(dataRequests);
            statusFuture = toCompletableFuture(dataSignalFuture);
        }
        // check if any data request is finished
        if (!dataSignalFuture.isDone()) {
            // not finished yet
            return null;
        }

        // at least one of data requests completed
        int index = nextCompletedRequest();
        PrestoThriftPageResult pageResult = getFutureValue(dataRequests.get(index));
        Page page = pageResult.toPage(outputColumnTypes);
        if (pageResult.getNextToken() != null) {
            // can get more data
            dataRequests.set(index, sendDataRequest(contexts.get(index), pageResult.getNextToken()));
            dataSignalFuture = whenAnyComplete(dataRequests);
            statusFuture = toCompletableFuture(dataSignalFuture);
            return page;
        }

        // split finished, closing the client, cleaning up context and completed future
        contexts.get(index).close();
        contexts.set(index, null);
        dataRequests.set(index, null);

        // are there more splits available
        if (splitIndex < splits.size()) {
            // can send data request for a new split
            PrestoThriftSplit split = splits.get(splitIndex);
            splitIndex++;
            RunningSplitContext context = new RunningSplitContext(openClient(split), split);
            contexts.set(index, context);
            dataRequests.set(index, sendDataRequest(context, null));
            dataSignalFuture = whenAnyComplete(dataRequests);
            statusFuture = toCompletableFuture(dataSignalFuture);
        }
        else if (!dataRequests.isEmpty()) {
            // no more new splits, but some requests are still in progress, wait for them
            dataSignalFuture = whenAnyComplete(dataRequests);
            statusFuture = toCompletableFuture(dataSignalFuture);
        }
        else {
            // all done: no more new splits, no requests in progress
            dataSignalFuture = null;
            statusFuture = null;
            finished = true;
        }
        return page;
    }

    private ListenableFuture<PrestoThriftSplitBatch> sendSplitRequest(PrestoThriftService client, @Nullable PrestoThriftId nextToken)
    {
        return client.getLookupSplits(schemaTableName, lookupColumnNames, outputColumnNames, keys, outputConstraint, MAX_SPLIT_COUNT, new PrestoThriftNullableToken(nextToken));
    }

    private ListenableFuture<PrestoThriftPageResult> sendDataRequest(RunningSplitContext context, @Nullable PrestoThriftId nextToken)
    {
        return context.getClient().getRows(context.getSplit().getSplitId(), outputColumnNames, MAX_BYTES, new PrestoThriftNullableToken(nextToken));
    }

    private PrestoThriftService openClient(PrestoThriftSplit split)
    {
        if (split.getHosts().isEmpty()) {
            return clientProvider.anyHostClient();
        }
        else {
            return clientProvider.selectedHostClient(toHostAddressList(split.getHosts()));
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        // cancel futures if available
        cancel(statusFuture);
        cancel(splitFuture);
        dataRequests.forEach(ThriftIndexPageSource::cancel);
        // close clients if available
        contexts.forEach(ThriftIndexPageSource::closeQuietly);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return statusFuture == null ? NOT_BLOCKED : statusFuture;
    }

    private int nextCompletedRequest()
    {
        // simulate round robin checking in the list of running requests
        // next time the search will continue from the position right after the last found one
        for (int i = 0; i < dataRequests.size(); i++) {
            lastFoundPosition = (lastFoundPosition + 1) % dataRequests.size();
            if (dataRequests.get(lastFoundPosition) != null && dataRequests.get(lastFoundPosition).isDone()) {
                return lastFoundPosition;
            }
        }
        throw new IllegalStateException("No completed splits in the queue");
    }

    private static void cancel(Future<?> future)
    {
        if (future != null) {
            future.cancel(true);
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (Exception e) {
            log.warn(e, "Error during close");
        }
    }

    private static final class RunningSplitContext
            implements Closeable
    {
        private final PrestoThriftService client;
        private final PrestoThriftSplit split;

        public RunningSplitContext(PrestoThriftService client, PrestoThriftSplit split)
        {
            this.client = client;
            this.split = split;
        }

        public PrestoThriftService getClient()
        {
            return client;
        }

        public PrestoThriftSplit getSplit()
        {
            return split;
        }

        @Override
        public void close()
        {
            client.close();
        }
    }
}
