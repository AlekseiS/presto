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

import com.facebook.presto.genericthrift.GenericThriftSplit;
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GenericThriftNormalPageSource
        extends GenericThriftAbstractPageSource
{
    private final byte[] splitId;
    private final ThriftPrestoClient client;

    public GenericThriftNormalPageSource(
            PrestoClientProvider clientProvider,
            GenericThriftSplit split,
            List<ColumnHandle> columns)
    {
        super(columns);
        requireNonNull(split, "split is null");
        this.splitId = split.getSplitId();
        if (split.getAddresses().isEmpty()) {
            this.client = clientProvider.connectToAnyHost();
        }
        else {
            this.client = clientProvider.connectToAnyOf(split.getAddresses());
        }
        requireNonNull(clientProvider, "clientProvider is null");
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, int maxRecords)
    {
        return client.getRows(splitId, maxRecords, nextToken);
    }

    @Override
    public void closeInternal()
    {
        client.close();
    }
}
