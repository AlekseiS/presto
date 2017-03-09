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

import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

public class GenericThriftNoColumnsPageSource
        implements ConnectorPageSource
{
    private static final int MAX_RECORDS_PER_REQUEST = 8192;
    private final GenericThriftSplit split;
    private final ThriftPrestoClient client;

    private byte[] nextToken;
    private boolean firstCall = true;
    private CompletableFuture<ThriftRowsBatch> future;

    public GenericThriftNoColumnsPageSource(PrestoClientProvider clientProvider, GenericThriftSplit split)
    {
        this.split = requireNonNull(split, "split is null");
        requireNonNull(clientProvider, "clientProvider is null");
        if (split.getAddresses().isEmpty()) {
            this.client = clientProvider.connectToAnyHost();
        }
        else {
            this.client = clientProvider.connectToAnyOf(split.getAddresses());
        }
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
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
        return !firstCall && nextToken == null;
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        if (future != null) {
            if (!future.isDone()) {
                return null;
            }
            else {
                ThriftRowsBatch response = getFutureValue(future);
                future = null;
                firstCall = false;
                nextToken = response.getNextToken();
                checkState(response.getRowCount() > 0 || nextToken == null,
                        "Batch cannot be empty when continuation token is present");
                return response.getRowCount() > 0 ? new Page(response.getRowCount()) : null;
            }
        }
        else {
            future = toCompletableFuture(client.getRows(split.getSplitId(), ImmutableList.of(), MAX_RECORDS_PER_REQUEST, nextToken));
            return null;
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return future == null || future.isDone() ? NOT_BLOCKED : future;
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
        if (future != null) {
            future.cancel(true);
        }
        client.close();
    }
}
