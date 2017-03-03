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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class HostsList
{
    private final List<HostAddress> hosts;

    private HostsList(List<HostAddress> hosts)
    {
        this.hosts = ImmutableList.copyOf(requireNonNull(hosts, "hosts is null"));
    }

    // needed for automatic config parsing
    @SuppressWarnings("unused")
    public static HostsList fromString(String hosts)
    {
        return new HostsList(Splitter.on(',').splitToList(hosts).stream().map(HostAddress::fromString).collect(toList()));
    }

    public static HostsList of(HostAddress... hosts)
    {
        return new HostsList(asList(hosts));
    }

    public static HostsList fromList(List<HostAddress> hosts)
    {
        return new HostsList(hosts);
    }

    public static HostsList empty()
    {
        return new HostsList(ImmutableList.of());
    }

    public List<HostAddress> getHosts()
    {
        return hosts;
    }

    public String stringValue()
    {
        return Joiner.on(',').join(hosts);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HostsList hostsList = (HostsList) o;

        return hosts.equals(hostsList.hosts);
    }

    @Override
    public int hashCode()
    {
        return hosts.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hosts", hosts)
                .toString();
    }
}
