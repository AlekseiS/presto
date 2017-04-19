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
import com.facebook.presto.thrift.ThriftConnectorConfig;
import com.facebook.presto.thrift.ThriftConnectorSplit;
import com.facebook.presto.thrift.clientproviders.ThriftServiceClientProvider;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftServiceClient;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftPageSource
        extends AbstractThriftPageSource
{
    private final byte[] splitId;
    private final ThriftServiceClient client;

    public ThriftPageSource(
            ThriftServiceClientProvider clientProvider,
            ThriftConnectorConfig config,
            ThriftConnectorSplit split,
            List<ColumnHandle> columns)
    {
        super(columns, config);
        requireNonNull(split, "split is null");
        this.splitId = split.getSplitId();
        requireNonNull(clientProvider, "clientProvider is null");
        if (split.getAddresses().isEmpty()) {
            this.client = clientProvider.anyHostClient();
        }
        else {
            this.client = clientProvider.selectedHostClient(split.getAddresses());
        }
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> sendDataRequest(byte[] nextToken, long maxBytes)
    {
        return client.getRows(splitId, maxBytes, nextToken);
    }

    @Override
    public void closeInternal()
    {
        client.close();
    }
}
