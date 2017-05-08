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
package com.facebook.presto.connector.thrift.api;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;

@ThriftStruct
public final class PrestoThriftSessionValue
{
    private final boolean nullValue;
    private final Long longValue;
    private final Integer intValue;
    private final Boolean booleanValue;
    private final Double doubleValue;
    private final String stringValue;

    @ThriftConstructor
    public PrestoThriftSessionValue(boolean nullValue, Long longValue, Integer intValue, Boolean booleanValue, Double doubleValue, String stringValue)
    {
        checkArgument(nullValue && allNulls(longValue, intValue, booleanValue, doubleValue, stringValue) ||
                !nullValue && exactlyOneNotNull(longValue, intValue, booleanValue, doubleValue, stringValue), "values combination must be valid");
        this.nullValue = nullValue;
        this.longValue = longValue;
        this.intValue = intValue;
        this.booleanValue = booleanValue;
        this.doubleValue = doubleValue;
        this.stringValue = stringValue;
    }

    @ThriftField(1)
    public boolean isNullValue()
    {
        return nullValue;
    }

    @ThriftField(value = 2, requiredness = OPTIONAL)
    public Long getLongValue()
    {
        return longValue;
    }

    @ThriftField(value = 3, requiredness = OPTIONAL)
    public Integer getIntValue()
    {
        return intValue;
    }

    @ThriftField(value = 4, requiredness = OPTIONAL)
    public Boolean getBooleanValue()
    {
        return booleanValue;
    }

    @ThriftField(value = 5, requiredness = OPTIONAL)
    public Double getDoubleValue()
    {
        return doubleValue;
    }

    @ThriftField(value = 6, requiredness = OPTIONAL)
    public String getStringValue()
    {
        return stringValue;
    }

    private static boolean allNulls(Object... values)
    {
        for (Object obj : values) {
            if (obj != null) {
                return false;
            }
        }
        return true;
    }

    private static boolean exactlyOneNotNull(Object... values)
    {
        int notNull = 0;
        for (Object obj : values) {
            if (obj != null) {
                notNull++;
            }
        }
        return notNull == 1;
    }
}
