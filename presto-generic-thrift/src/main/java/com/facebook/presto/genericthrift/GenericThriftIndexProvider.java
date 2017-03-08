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

import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GenericThriftIndexProvider
        implements ConnectorIndexProvider
{
    private final PrestoClientProvider clientProvider;

    @Inject
    public GenericThriftIndexProvider(PrestoClientProvider clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    }

    @Override
    public ConnectorIndex getIndex(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema,
            List<ColumnHandle> outputSchema)
    {
        GenericThriftIndexHandle thriftIndexHandle = (GenericThriftIndexHandle) indexHandle;
        return new GenericThriftConnectorIndex(clientProvider, thriftIndexHandle, lookupSchema, outputSchema);
    }
}
