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

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.location.ThriftLocationProvider;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.swift.service.ThriftClient;
import com.google.common.net.HostAndPort;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.genericthrift.GenericThriftErrorCode.CONNECTION_ERROR;
import static java.util.Objects.requireNonNull;

public class PrestoThriftClientProvider
        implements PrestoClientProvider
{
    private static final long THRIFT_CONNECT_FUTURE_TIMEOUT_MS = 31_000;
    private final ThriftClient<ThriftPrestoClient> thriftClient;
    private final ThriftLocationProvider locationProvider;

    @Inject
    public PrestoThriftClientProvider(ThriftClient<ThriftPrestoClient> thriftClient, ThriftLocationProvider locationProvider)
    {
        this.thriftClient = requireNonNull(thriftClient, "thriftClient is null");
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
    }

    @Override
    public ThriftPrestoClient connectToAnyHost()
    {
        return connectTo(locationProvider.getAnyHost());
    }

    @Override
    public ThriftPrestoClient connectToAnyOf(List<HostAddress> hosts)
    {
        return connectTo(locationProvider.getAnyOf(hosts));
    }

    private ThriftPrestoClient connectTo(HostAddress host)
    {
        try {
            return thriftClient.open(new FramedClientConnector(HostAndPort.fromParts(host.getHostText(), host.getPort())))
                    .get(THRIFT_CONNECT_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new PrestoException(CONNECTION_ERROR, "Cannot connect to thrift host at " + host, e);
        }
    }
}
