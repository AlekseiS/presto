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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class QueryActions
{
    private final Map<String, String> setSessionProperties;
    private final Set<String> clearSessionProperties;
    private final Map<String, String> addedPreparedStatements;
    private final Set<String> deallocatedPreparedStatements;
    private final String startedTransactionId;
    private final Boolean clearTransactionId;

    @JsonCreator
    public QueryActions(
            @JsonProperty("setSessionProperties") Map<String, String> setSessionProperties,
            @JsonProperty("clearSessionProperties") Set<String> clearSessionProperties,
            @JsonProperty("addedPreparedStatements") Map<String, String> addedPreparedStatements,
            @JsonProperty("deallocatedPreparedStatements") Set<String> deallocatedPreparedStatements,
            @JsonProperty("startedTransactionId") String startedTransactionId,
            @JsonProperty("clearTransactionId") Boolean clearTransactionId)
    {
        this.setSessionProperties = setSessionProperties != null ? ImmutableMap.copyOf(setSessionProperties) : null;
        this.clearSessionProperties = clearSessionProperties != null ? ImmutableSet.copyOf(clearSessionProperties) : null;
        this.addedPreparedStatements = addedPreparedStatements != null ? ImmutableMap.copyOf(addedPreparedStatements) : null;
        this.deallocatedPreparedStatements = deallocatedPreparedStatements != null ? ImmutableSet.copyOf(deallocatedPreparedStatements) : null;
        this.startedTransactionId = startedTransactionId;
        this.clearTransactionId = clearTransactionId;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    @Nullable
    @JsonProperty
    public Set<String> getClearSessionProperties()
    {
        return clearSessionProperties;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    @Nullable
    @JsonProperty
    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    @Nullable
    @JsonProperty
    public String getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Nullable
    @JsonProperty
    public Boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("setSessionProperties", setSessionProperties)
                .add("clearSessionProperties", clearSessionProperties)
                .add("addedPreparedStatements", addedPreparedStatements)
                .add("deallocatedPreparedStatements", deallocatedPreparedStatements)
                .add("startedTransactionId", startedTransactionId)
                .add("clearTransactionId", clearTransactionId)
                .toString();
    }

    @Nullable
    public static QueryActions createIfNecessary(
            Map<String, String> setSessionProperties,
            Set<String> clearSessionProperties,
            Map<String, String> addedPreparedStatements,
            Set<String> deallocatedPreparedStatements,
            Optional<String> startedTransactionId,
            boolean clearTransactionId)
    {
        if (setSessionProperties.isEmpty() &&
                clearSessionProperties.isEmpty() &&
                addedPreparedStatements.isEmpty() &&
                deallocatedPreparedStatements.isEmpty() &&
                !startedTransactionId.isPresent() &&
                !clearTransactionId) {
            // query actions would be empty
            return null;
        }
        return new QueryActions(
                setSessionProperties.isEmpty() ? null : setSessionProperties,
                clearSessionProperties.isEmpty() ? null : clearSessionProperties,
                addedPreparedStatements.isEmpty() ? null : addedPreparedStatements,
                deallocatedPreparedStatements.isEmpty() ? null : deallocatedPreparedStatements,
                startedTransactionId.orElse(null),
                clearTransactionId ? true : null);
    }
}
