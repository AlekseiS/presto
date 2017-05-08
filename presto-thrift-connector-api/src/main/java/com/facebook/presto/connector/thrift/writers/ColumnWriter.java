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
package com.facebook.presto.connector.thrift.writers;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public interface ColumnWriter
{
    void append(RecordCursor cursor, int field);

    void append(Block block, int position, Type type);

    /**
     * Return column data structure with the written data.
     * Note that a writer can return several columns when used with structural types, like array or map.
     * This method is expected to be called exactly once per lifecycle of the writer.
     */
    List<PrestoThriftColumnData> getResult();
}
