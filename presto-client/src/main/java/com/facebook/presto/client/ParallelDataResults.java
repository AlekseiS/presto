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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Objects.requireNonNull;

@Immutable
public class ParallelDataResults
{
    private final String id;
    private final List<Column> columns;
    private final Iterable<List<Object>> data;

    @JsonCreator
    public ParallelDataResults(
            @JsonProperty("id") String id,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data)
    {
        this(id, columns, QueryResults.fixData(columns, data));
    }

    public ParallelDataResults(
            String id,
            List<Column> columns,
            Iterable<List<Object>> data)
    {
        this.id = requireNonNull(id, "id is null");
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
    }

    @NotNull
    @JsonProperty
    public String getId()
    {
        return id;
    }

    @Nullable
    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("columns", columns)
                .add("hasData", data != null)
                .toString();
    }
}
