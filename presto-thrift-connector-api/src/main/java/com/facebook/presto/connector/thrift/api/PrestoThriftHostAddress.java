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
package com.facebook.presto.connector.thrift.api;

import com.facebook.presto.spi.HostAddress;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftHostAddress
{
    private final String host;
    private final int port;

    @ThriftConstructor
    public PrestoThriftHostAddress(
            @ThriftField(name = "host") String host,
            @ThriftField(name = "port") int port)
    {
        this.host = requireNonNull(host, "host is null");
        this.port = port;
    }

    @ThriftField(1)
    public String getHost()
    {
        return host;
    }

    @ThriftField(2)
    public int getPort()
    {
        return port;
    }

    public HostAddress toHostAddress()
    {
        return HostAddress.fromParts(getHost(), getPort());
    }

    public static List<HostAddress> toHostAddressList(List<PrestoThriftHostAddress> hosts)
    {
        return hosts.stream().map(PrestoThriftHostAddress::toHostAddress).collect(toImmutableList());
    }
}
