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

import com.facebook.presto.genericthrift.client.ThriftConnectorSession;
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftSchemaTableName;
import com.facebook.presto.genericthrift.client.ThriftSplit;
import com.facebook.presto.genericthrift.client.ThriftSplitBatch;
import com.facebook.presto.genericthrift.client.ThriftTableLayout;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.genericthrift.client.ThriftConnectorSession.fromConnectorSession;
import static com.facebook.presto.genericthrift.client.ThriftSchemaTableName.fromSchemaTableName;
import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.fromTupleDomain;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public class GenericThriftSplitManager
        implements ConnectorSplitManager
{
    private final PrestoClientProvider clientProvider;

    @Inject
    public GenericThriftSplitManager(PrestoClientProvider clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        GenericThriftTableLayoutHandle layoutHandle = (GenericThriftTableLayoutHandle) layout;
        return new GenericThriftSplitSource(
                clientProvider.connectToAnyHost(),
                fromConnectorSession(session),
                fromSchemaTableName(layoutHandle.getSchemaTableName()),
                new ThriftTableLayout(
                        layoutHandle.getOutputColumns()
                                .map(columns -> columns
                                        .stream()
                                        .map(column -> ((GenericThriftColumnHandle) column).getColumnName())
                                        .collect(toList()))
                                .orElse(null),
                        fromTupleDomain(layoutHandle.getPredicate())));
    }

    private static class GenericThriftSplitSource
            implements ConnectorSplitSource
    {
        private final ThriftPrestoClient client;
        private final ThriftConnectorSession session;
        private final ThriftSchemaTableName schemaTableName;
        private final ThriftTableLayout layout;

        private String continuationToken;
        private boolean firstCall = true;

        public GenericThriftSplitSource(
                ThriftPrestoClient client,
                ThriftConnectorSession session,
                ThriftSchemaTableName schemaTableName,
                ThriftTableLayout layout)
        {
            this.client = requireNonNull(client, "client is null");
            this.session = requireNonNull(session, "session is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.layout = requireNonNull(layout, "layout is null");
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            checkState(firstCall || continuationToken != null);
            ThriftSplitBatch batch = client.getSplitBatch(session, schemaTableName, layout, maxSize, continuationToken);
            List<ConnectorSplit> splits = batch.getSplits().stream().map(ThriftSplit::toConnectorSplit).collect(toList());
            continuationToken = batch.getNextToken();
            firstCall = false;
            return completedFuture(splits);
        }

        @Override
        public boolean isFinished()
        {
            return continuationToken == null && !firstCall;
        }

        @Override
        public void close()
        {
            client.close();
        }
    }
}
