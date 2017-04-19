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
package com.facebook.presto.thrift.integration;

import com.facebook.presto.tests.AbstractTestIndexedQueries;
import com.facebook.swift.service.ThriftServer;
import com.google.common.io.Closeables;
import org.testng.annotations.AfterClass;

import java.util.List;

import static com.facebook.presto.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static com.facebook.presto.thrift.integration.ThriftQueryRunner.startThriftServers;
import static java.util.Objects.requireNonNull;

public class TestThriftDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    private final List<ThriftServer> thriftServers;

    public TestThriftDistributedQueriesIndexed()
            throws Exception
    {
        this(startThriftServers());
    }

    public TestThriftDistributedQueriesIndexed(List<ThriftServer> servers)
            throws Exception
    {
        super(() -> createThriftQueryRunner(servers));
        this.thriftServers = requireNonNull(servers, "servers is null");
    }

    @Override
    public void testExampleSystemTable()
            throws Exception
    {
        // system tables are not supported
    }

    @AfterClass(alwaysRun = true)
    @SuppressWarnings({"EmptyTryBlock", "UnusedDeclaration"})
    public void tearDown()
            throws Exception
    {
        for (ThriftServer server : thriftServers) {
            Closeables.close(server, true);
        }
    }
}
