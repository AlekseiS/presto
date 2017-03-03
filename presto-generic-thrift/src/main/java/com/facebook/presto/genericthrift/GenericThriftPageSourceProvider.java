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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GenericThriftPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final PrestoClientProvider clientProvider;

    @Inject
    public GenericThriftPageSourceProvider(PrestoClientProvider clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            return new GenericThriftNoColumnsPageSource(clientProvider, (GenericThriftSplit) split);
        }
        else {
            List<GenericThriftColumnHandle> genericThriftColumns = columns.stream().map(columnHandle -> (GenericThriftColumnHandle) columnHandle).collect(toList());
            return new GenericThriftPageSource(clientProvider, (GenericThriftSplit) split, genericThriftColumns);
        }
    }
}
