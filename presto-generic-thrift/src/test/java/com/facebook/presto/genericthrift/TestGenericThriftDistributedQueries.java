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

import com.facebook.presto.Session;
import com.facebook.presto.genericthrift.location.HostsList;
import com.facebook.presto.genericthrift.testimpl.ThriftPrestoClientTpch;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import org.testng.annotations.AfterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TestGenericThriftDistributedQueries
        extends AbstractTestQueries
{
    private static final int DEFAULT_THRIFT_NODES_COUNT = 3;
    private static final int DEFAULT_WORKERS_COUNT = 4;
    private final List<ThriftServer> thriftServers;

    public TestGenericThriftDistributedQueries()
            throws Exception
    {
        this(startThriftServers(DEFAULT_THRIFT_NODES_COUNT));
    }

    public TestGenericThriftDistributedQueries(List<ThriftServer> servers)
            throws Exception
    {
        super(createQueryRunner(servers));
        this.thriftServers = requireNonNull(servers, "servers is null");
    }

    private static List<ThriftServer> startThriftServers(int nodes)
    {
        List<ThriftServer> servers = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            ThriftServiceProcessor processor = new ThriftServiceProcessor(new ThriftCodecManager(), ImmutableList.of(), new ThriftPrestoClientTpch());

            servers.add(new ThriftServer(processor).start());
        }
        return servers;
    }

    private static QueryRunner createQueryRunner(List<ThriftServer> servers)
            throws Exception
    {
        List<HostAddress> addresses = servers.stream().map(server -> HostAddress.fromParts("localhost", server.getPort())).collect(toList());
        HostsList hosts = HostsList.fromList(addresses);

        Session defaultSession = testSessionBuilder()
                .setCatalog("genericthrift")
                .setSchema("tiny")
                .build();
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(defaultSession, DEFAULT_WORKERS_COUNT);
        queryRunner.installPlugin(new GenericThriftPlugin());
        Map<String, String> genericThriftProperties = ImmutableMap.of(
                "static-location.hosts", hosts.stringValue(),
                "presto-generic-thrift.thrift.client.connect-timeout", "30s"
        );
        queryRunner.createCatalog("genericthrift", "generic-thrift", genericThriftProperties);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    @SuppressWarnings({"EmptyTryBlock", "UnusedDeclaration"})
    public void tearDown()
            throws Exception
    {
        Closeables.close(queryRunner, true);
        for (ThriftServer server : thriftServers) {
            Closeables.close(server, true);
        }
    }
}
