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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tests.tpch.TpchIndexedData.IndexedTable;
import com.google.common.base.Function;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;

class TpchConnectorIndex
        implements ConnectorIndex
{
    private final Function<RecordSet, RecordSet> keyFormatter;
    private final Function<RecordSet, RecordSet> outputFormatter;
    private final IndexedTable indexedTable;

    public TpchConnectorIndex(Function<RecordSet, RecordSet> keyFormatter, Function<RecordSet, RecordSet> outputFormatter, IndexedTable indexedTable)
    {
        this.keyFormatter = requireNonNull(keyFormatter, "keyFormatter is null");
        this.outputFormatter = requireNonNull(outputFormatter, "outputFormatter is null");
        this.indexedTable = requireNonNull(indexedTable, "indexedTable is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet rawInputRecordSet)
    {
        int totalRecords = totalRecords(rawInputRecordSet);
        System.err.println("Total records in input: " + totalRecords);
        // convert the input record set from the column ordering in the query to
        // match the column ordering of the index
        RecordSet inputRecordSet = keyFormatter.apply(rawInputRecordSet);

        // lookup the values in the index
        RecordSet rawOutputRecordSet = new ShuffledRecordSet(indexedTable.lookupKeys(inputRecordSet));

        // convert the output record set of the index into the column ordering
        // expect by the query
        return new RecordPageSource(outputFormatter.apply(rawOutputRecordSet));
    }

    private static int totalRecords(RecordSet recordSet)
    {
        RecordCursor cursor = recordSet.cursor();
        int result = 0;
        while (cursor.advanceNextPosition()) {
            result++;
        }
        return result;
    }

    private static final class ShuffledRecordSet
            implements RecordSet
    {
        private final RecordSet recordSet;
        private final List<Integer> positions;

        public ShuffledRecordSet(RecordSet recordSet)
        {
            this.recordSet = requireNonNull(recordSet, "recordSet is null");
            int totalRecords = totalRecords(recordSet);
            System.err.println("Total records in output: " + totalRecords);
            this.positions = new ArrayList<>(totalRecords);
            for (int i = 0; i < totalRecords; i++) {
                positions.add(i);
            }
            shuffle(positions);
        }

        @Override
        public List<Type> getColumnTypes()
        {
            return recordSet.getColumnTypes();
        }

        @Override
        public RecordCursor cursor()
        {
            return new ShuffledRecordCursor();
        }

        private class ShuffledRecordCursor
                implements RecordCursor
        {
            private RecordCursor cursor;
            private int currentPosition;
            private int positionIndex = -1;

            private ShuffledRecordCursor()
            {
                resetCursor();
            }

            private void resetCursor()
            {
                if (cursor != null) {
                    cursor.close();
                }
                cursor = recordSet.cursor();
                currentPosition = -1;
            }

            private void seekTo(int newPosition)
            {
                if (newPosition < currentPosition) {
                    resetCursor();
                }
                while (newPosition > currentPosition) {
                    checkState(cursor.advanceNextPosition());
                    currentPosition++;
                }
            }

            @Override
            public long getCompletedBytes()
            {
                return 0;
            }

            @Override
            public long getReadTimeNanos()
            {
                return 0;
            }

            @Override
            public Type getType(int field)
            {
                return recordSet.getColumnTypes().get(field);
            }

            @Override
            public boolean advanceNextPosition()
            {
                if (positionIndex + 1 >= positions.size()) {
                    return false;
                }
                positionIndex++;
                seekTo(positions.get(positionIndex));
                return true;
            }

            @Override
            public boolean getBoolean(int field)
            {
                return cursor.getBoolean(field);
            }

            @Override
            public long getLong(int field)
            {
                return cursor.getLong(field);
            }

            @Override
            public double getDouble(int field)
            {
                return cursor.getDouble(field);
            }

            @Override
            public Slice getSlice(int field)
            {
                return cursor.getSlice(field);
            }

            @Override
            public Object getObject(int field)
            {
                return cursor.getObject(field);
            }

            @Override
            public boolean isNull(int field)
            {
                return cursor.isNull(field);
            }

            @Override
            public void close()
            {
                cursor.close();
            }
        }
    }
}
