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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class ThriftIndexPageSource
        implements ConnectorPageSource
{

    public ThriftIndexPageSource()
    {
    }

    @Override
    public long getCompletedBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReadTimeNanos()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinished()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getNextPage()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        throw new UnsupportedOperationException();
    }
}
