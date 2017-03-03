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
package com.facebook.presto.genericthrift.client;

import com.facebook.presto.spi.predicate.AllOrNoneValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public final class ThriftAllOrNoneValueSet
{
    private final boolean all;

    @ThriftConstructor
    public ThriftAllOrNoneValueSet(boolean all)
    {
        this.all = all;
    }

    @ThriftField(1)
    public boolean isAll()
    {
        return all;
    }

    public static AllOrNoneValueSet toAllOrNoneValueSet(ThriftAllOrNoneValueSet valueSet, Type type)
    {
        return new AllOrNoneValueSet(type, valueSet.isAll());
    }

    public static ThriftAllOrNoneValueSet fromAllOrNoneValueSet(AllOrNoneValueSet valueSet)
    {
        return new ThriftAllOrNoneValueSet(valueSet.isAll());
    }
}
