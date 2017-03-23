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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.swift.service.ThriftServer;
import com.google.common.io.Closeables;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.thrift.integration.ThriftQueryRunnerUtils.createQueryRunner;
import static com.facebook.presto.thrift.integration.ThriftQueryRunnerUtils.startThriftServers;
import static java.util.Objects.requireNonNull;

public class TestThriftIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final List<ThriftServer> thriftServers;

    public TestThriftIntegrationSmokeTest()
            throws Exception
    {
        this(startThriftServers(2));
    }

    public TestThriftIntegrationSmokeTest(List<ThriftServer> servers)
            throws Exception
    {
        super(() -> createQueryRunner(servers, 2));
        this.thriftServers = requireNonNull(servers, "servers is null");
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

    @Override
    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toJdbcTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row("tiny")
                .row("sf1");
        assertContains(actualSchemas, resultBuilder.build());
    }
}
