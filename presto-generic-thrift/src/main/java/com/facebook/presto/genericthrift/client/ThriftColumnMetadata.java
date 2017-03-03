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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftColumnMetadata
{
    private final String name;
    private final String type;
    private final String comment;
    private final boolean hidden;

    @ThriftConstructor
    public ThriftColumnMetadata(String name, String type, @Nullable String comment, boolean hidden)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = comment;
        this.hidden = hidden;
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(2)
    public String getType()
    {
        return type;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public String getComment()
    {
        return comment;
    }

    @ThriftField(4)
    public boolean isHidden()
    {
        return hidden;
    }

    public static ColumnMetadata toColumnMetadata(ThriftColumnMetadata thriftColumnMetadata, TypeManager typeManager)
    {
        return new ColumnMetadata(thriftColumnMetadata.getName(),
                typeManager.getType(parseTypeSignature(thriftColumnMetadata.getType())),
                thriftColumnMetadata.getComment(),
                thriftColumnMetadata.isHidden());
    }
}
