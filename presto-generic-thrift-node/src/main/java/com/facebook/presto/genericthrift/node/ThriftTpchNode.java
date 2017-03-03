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
package com.facebook.presto.genericthrift.node;

import com.facebook.swift.codec.guice.ThriftCodecModule;
import com.facebook.swift.service.guice.ThriftClientModule;
import com.facebook.swift.service.guice.ThriftServerModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;

import java.util.List;

import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

public final class ThriftTpchNode
{
    private final List<Module> modules;
    private final List<Module> extraModules;

    public ThriftTpchNode()
    {
        this(ImmutableList.of());
    }

    public ThriftTpchNode(List<Module> extraModules)
    {
        this.extraModules = ImmutableList.copyOf(requireNonNull(extraModules, "extraModules is null"));
        this.modules = ImmutableList.of(
                new ThriftCodecModule(),
                new ThriftClientModule(),
                new ThriftServerModule(),
                new ThriftTpchNodeModule());
    }

    public void run()
    {
        Logger log = Logger.get(ThriftTpchNode.class);

        Bootstrap app = new Bootstrap(concat(modules, extraModules));

        try {
            app.strictConfig().initialize();
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }

    public static void main(String[] args)
    {
        new ThriftTpchNode().run();
    }
}
