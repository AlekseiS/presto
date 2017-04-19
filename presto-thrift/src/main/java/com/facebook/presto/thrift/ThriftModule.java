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
package com.facebook.presto.thrift;

import com.facebook.presto.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.thrift.annotations.NonRetrying;
import com.facebook.presto.thrift.clientproviders.DefaultThriftServiceClientProvider;
import com.facebook.presto.thrift.clientproviders.RetryingThriftServiceClientProvider;
import com.facebook.presto.thrift.clientproviders.ThriftServiceClientProvider;
import com.facebook.presto.thrift.interfaces.client.ThriftServiceClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.util.concurrent.Executor;

import static com.facebook.swift.service.guice.ThriftClientBinder.thriftClientBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ThriftModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ThriftConnector.class).in(Scopes.SINGLETON);
        thriftClientBinder(binder).bindThriftClient(ThriftServiceClient.class);
        binder.bind(ThriftMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ThriftSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ThriftPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ThriftServiceClientProvider.class).to(RetryingThriftServiceClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(ThriftServiceClientProvider.class).annotatedWith(NonRetrying.class)
                .to(DefaultThriftServiceClientProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ThriftConnectorConfig.class);
        binder.bind(ThriftInternalSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ThriftClientSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ThriftIndexProvider.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForMetadataRefresh
    public Executor createMetadataRefreshExecutor(ThriftConnectorConfig config)
    {
        return newFixedThreadPool(config.getMetadataRefreshThreads(), daemonThreadsNamed("metadata-refresh-%s"));
    }
}
