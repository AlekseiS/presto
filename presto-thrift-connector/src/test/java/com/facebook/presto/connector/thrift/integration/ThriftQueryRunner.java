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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.Session;
import com.facebook.presto.connector.thrift.ThriftPlugin;
import com.facebook.presto.connector.thrift.location.HostList;
import com.facebook.presto.connector.thrift.server.ThriftTpchService;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class ThriftQueryRunner
{
    public static final int DEFAULT_WORKERS_COUNT = 3;
    public static final int DEFAULT_THRIFT_NODES_COUNT = 3;

    private ThriftQueryRunner() {}

    public static QueryRunner createThriftQueryRunner(List<ThriftServer> servers)
            throws Exception
    {
        return createThriftQueryRunner(servers, DEFAULT_WORKERS_COUNT);
    }

    public static QueryRunner createThriftQueryRunner(List<ThriftServer> servers, int workers)
            throws Exception
    {
        List<HostAddress> addresses = servers
                .stream()
                .map(server -> HostAddress.fromParts("localhost", server.getPort()))
                .collect(toImmutableList());
        HostList hosts = HostList.fromList(addresses);

        Session defaultSession = testSessionBuilder()
                .setCatalog("thrift")
                .setSchema("tiny")
                .build();
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(defaultSession, workers);
        queryRunner.installPlugin(new ThriftPlugin());
        Map<String, String> connectorProperties = ImmutableMap.of(
                "static-location.hosts", hosts.stringValue(),
                "PrestoThriftService.thrift.client.connect-timeout", "30s"
        );
        queryRunner.createCatalog("thrift", "presto-thrift", connectorProperties);
        return queryRunner;
    }

    public static List<ThriftServer> startThriftServers()
    {
        return startThriftServers(DEFAULT_THRIFT_NODES_COUNT);
    }

    public static List<ThriftServer> startThriftServers(int nodes)
    {
        List<ThriftServer> servers = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            ThriftServiceProcessor processor = new ThriftServiceProcessor(new ThriftCodecManager(), ImmutableList.of(), new ThriftTpchService());
            servers.add(new ThriftServer(processor).start());
        }
        return servers;
    }
}
