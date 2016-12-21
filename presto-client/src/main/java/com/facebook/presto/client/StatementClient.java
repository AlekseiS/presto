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
package com.facebook.presto.client;

import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public interface StatementClient
        extends Closeable
{
    String getQuery();

    String getTimeZoneId();

    boolean isDebug();

    boolean isClosed();

    boolean isGone();

    boolean isFailed();

    StatementStats getStats();

    QueryResults current();

    QueryResults finalResults();

    Map<String, String> getSetSessionProperties();

    Set<String> getResetSessionProperties();

    Map<String, String> getAddedPreparedStatements();

    Set<String> getDeallocatedPreparedStatements();

    String getStartedtransactionId();

    boolean isClearTransactionId();

    boolean isValid();

    boolean advance();

    boolean cancelLeafStage(Duration timeout);

    @Override
    void close();
}
