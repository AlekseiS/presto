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
package com.facebook.presto.connector.thrift.api.datatypes;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.spi.block.Block;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.connector.thrift.api.PrestoThriftColumnData.bigintData;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.util.Collections.unmodifiableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoThriftBigint
{
    @Test
    public void testReadBlock()
            throws Exception
    {
        PrestoThriftColumnData columnsData = longColumn(
                new boolean[] {false, true, false, false, false, false, true},
                new long[] {2, 0, 1, 3, 8, 4, 0}
        );
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(2L, null, 1L, 3L, 8L, 4L, null));
    }

    @Test
    public void testReadBlockAllNullsOption1()
    {
        PrestoThriftColumnData columnsData = longColumn(
                new boolean[] {true, true, true, true, true, true, true},
                null
        );
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(null, null, null, null, null, null, null));
    }

    @Test
    public void testReadBlockAllNullsOption2()
    {
        PrestoThriftColumnData columnsData = longColumn(
                new boolean[] {true, true, true, true, true, true, true},
                new long[] {0, 0, 0, 0, 0, 0, 0}
        );
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(null, null, null, null, null, null, null));
    }

    @Test
    public void testReadBlockAllNonNullOption1()
            throws Exception
    {
        PrestoThriftColumnData columnsData = longColumn(
                null,
                new long[] {2, 7, 1, 3, 8, 4, 5}
        );
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(2L, 7L, 1L, 3L, 8L, 4L, 5L));
    }

    @Test
    public void testReadBlockAllNonNullOption2()
            throws Exception
    {
        PrestoThriftColumnData columnsData = longColumn(
                new boolean[] {false, false, false, false, false, false, false},
                new long[] {2, 7, 1, 3, 8, 4, 5}
        );
        Block actual = columnsData.toBlock(BIGINT);
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

    private static PrestoThriftColumnData longColumn(boolean[] nulls, long[] longs)
    {
        return bigintData(new PrestoThriftBigint(nulls, longs));
    }

    private static List<Long> list(Long... values)
    {
        return unmodifiableList(Arrays.asList(values));
    }
}
