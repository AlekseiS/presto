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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GenericThriftConnectorIndex
        implements ConnectorIndex
{
    private final PrestoClientProvider clientProvider;
    private final byte[] indexId;
    private final List<ColumnHandle> outputColumns;

    private String continuationToken;

    public GenericThriftConnectorIndex(PrestoClientProvider clientProvider, GenericThriftIndexHandle indexHandle, List<ColumnHandle> lookupColumns, List<ColumnHandle> outputColumns)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.indexId = requireNonNull(indexHandle, "indexHandle is null").getIndexId();
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
