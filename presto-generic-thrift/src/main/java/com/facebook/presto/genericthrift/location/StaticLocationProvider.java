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
package com.facebook.presto.genericthrift.location;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StaticLocationProvider
        implements ThriftLocationProvider
{
    private final Random random = new Random();
    private final List<HostAddress> hosts;

    @Inject
    public StaticLocationProvider(StaticLocationConfig config)
    {
        requireNonNull(config, "config is null");
        List<HostAddress> hosts = config.getHosts().getHosts();
        checkArgument(!hosts.isEmpty(), "hosts is empty");
        this.hosts = ImmutableList.copyOf(hosts);
    }

    @Override
    public HostAddress getAnyHost()
    {
        return hosts.get(random.nextInt(hosts.size()));
    }

    @Override
    public HostAddress getAnyOf(List<HostAddress> requestedHosts)
    {
        checkArgument(requestedHosts != null && !requestedHosts.isEmpty(), "requestedHosts is null or empty");
        return requestedHosts.get(random.nextInt(requestedHosts.size()));
    }
}
