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

import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftConnectorIndex
        implements ConnectorIndex
{
    private final PrestoThriftServiceProvider clientProvider;
    private final ThriftIndexHandle indexHandle;
    private final List<ColumnHandle> lookupColumns;
    private final List<ColumnHandle> outputColumns;

    public ThriftConnectorIndex(
            PrestoThriftServiceProvider clientProvider,
            ThriftIndexHandle indexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.lookupColumns = requireNonNull(lookupColumns, "lookupColumns is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        return new ThriftIndexPageSource();
    }
}
