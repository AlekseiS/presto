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
package com.facebook.presto.server.protocol;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.block.BlockEncodingSerde;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

class ActiveQueryUpdate
        extends ActiveQuery
{
    ActiveQueryUpdate(QueryInfo queryInfo, QueryManager queryManager, SessionPropertyManager sessionPropertyManager, ExchangeClient exchangeClient, Executor resultsProcessorExecutor, ScheduledExecutorService timeoutExecutor, BlockEncodingSerde blockEncodingSerde)
    {
        super(queryInfo, queryManager, sessionPropertyManager, exchangeClient, resultsProcessorExecutor, timeoutExecutor, blockEncodingSerde);
    }
}
