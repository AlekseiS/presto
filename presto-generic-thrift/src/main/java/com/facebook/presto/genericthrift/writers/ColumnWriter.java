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
package com.facebook.presto.genericthrift.writers;

import com.facebook.presto.genericthrift.client.ThriftColumnData;
import com.facebook.presto.spi.RecordCursor;

import java.util.List;

public interface ColumnWriter
{
    void append(RecordCursor cursor, int field);

    // a writer can return several columns when used with structural types, like array or map
    List<ThriftColumnData> getResult();
}
