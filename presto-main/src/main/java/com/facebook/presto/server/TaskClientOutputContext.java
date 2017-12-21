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
package com.facebook.presto.server;

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TaskClientOutputContext
{
    private final List<Type> types;
    private final PagesSerde pagesSerde;
    private final ConnectorSession connectorSession;

    public TaskClientOutputContext(List<Type> types, PagesSerde pagesSerde, ConnectorSession connectorSession)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public PagesSerde getPagesSerde()
    {
        return pagesSerde;
    }

    public ConnectorSession getConnectorSession()
    {
        return connectorSession;
    }
}
