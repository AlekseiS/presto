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
package com.facebook.presto.thrift.interfaces.readers;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLongColumnReader
{
    private static final String COLUMN_NAME = "column1";

    @Test
    public void testReadBlock()
            throws Exception
    {
        List<ThriftColumnData> columnsData = singleLongColumn(
                new boolean[] {false, true, false, false, false, false, true},
                new long[] {2, 0, 1, 3, 8, 4, 0}
        );
        Block actual = LongColumnReader.readBlock(columnsData, COLUMN_NAME, 7);
        assertBlockEquals(actual, list(2L, null, 1L, 3L, 8L, 4L, null));
    }

    @Test
    public void testReadBlockMultipleColumns()
            throws Exception
    {
        List<ThriftColumnData> columnsData = ImmutableList.of(
                longColumn(new boolean[] {false, true, false}, new long[] {1, 0, 2}, "column1"),
                longColumn(new boolean[] {false, true, false}, new long[] {10, 0, 20}, "column2"),
                longColumn(new boolean[] {false, true, false}, new long[] {100, 0, 200}, "column3")
        );
        Block actual1 = LongColumnReader.readBlock(columnsData, "column1", 3);
        assertBlockEquals(actual1, list(1L, null, 2L));
        Block actual2 = LongColumnReader.readBlock(columnsData, "column2", 3);
        assertBlockEquals(actual2, list(10L, null, 20L));
        Block actual3 = LongColumnReader.readBlock(columnsData, "column3", 3);
        assertBlockEquals(actual3, list(100L, null, 200L));
    }

    @Test
    public void testReadBlockAllNullsOption1()
    {
        List<ThriftColumnData> columnsData = singleLongColumn(
                new boolean[] {true, true, true, true, true, true, true},
                null
        );
        Block actual = LongColumnReader.readBlock(columnsData, COLUMN_NAME, 7);
        assertBlockEquals(actual, list(null, null, null, null, null, null, null));
    }

    @Test
    public void testReadBlockAllNullsOption2()
    {
        List<ThriftColumnData> columnsData = singleLongColumn(
                new boolean[] {true, true, true, true, true, true, true},
                new long[] {0, 0, 0, 0, 0, 0, 0}
        );
        Block actual = LongColumnReader.readBlock(columnsData, COLUMN_NAME, 7);
        assertBlockEquals(actual, list(null, null, null, null, null, null, null));
    }

    @Test
    public void testReadBlockAllNonNullOption1()
            throws Exception
    {
        List<ThriftColumnData> columnsData = singleLongColumn(
                null,
                new long[] {2, 7, 1, 3, 8, 4, 5}
        );
        Block actual = LongColumnReader.readBlock(columnsData, COLUMN_NAME, 7);
        assertBlockEquals(actual, list(2L, 7L, 1L, 3L, 8L, 4L, 5L));
    }

    @Test
    public void testReadBlockAllNonNullOption2()
            throws Exception
    {
        List<ThriftColumnData> columnsData = singleLongColumn(
                new boolean[] {false, false, false, false, false, false, false},
                new long[] {2, 7, 1, 3, 8, 4, 5}
        );
        Block actual = LongColumnReader.readBlock(columnsData, COLUMN_NAME, 7);
        assertBlockEquals(actual, list(2L, 7L, 1L, 3L, 8L, 4L, 5L));
    }

    private void assertBlockEquals(Block block, List<Long> expected)
    {
        assertEquals(block.getPositionCount(), expected.size());
        for (int i = 0; i < expected.size(); i++) {
            if (expected.get(i) == null) {
                assertTrue(block.isNull(i));
            }
            else {
                assertEquals(block.getLong(i, 0), expected.get(i).longValue());
            }
        }
    }

    private static List<ThriftColumnData> singleLongColumn(boolean[] nulls, long[] longs)
    {
        return ImmutableList.of(longColumn(nulls, longs, COLUMN_NAME));
    }

    private static ThriftColumnData longColumn(boolean[] nulls, long[] longs, String columnName)
    {
        return new ThriftColumnData(
                nulls,
                longs,
                null,
                null,
                null,
                columnName
        );
    }

    private static List<Long> list(Long... values)
    {
        return unmodifiableList(Arrays.asList(values));
    }
}
