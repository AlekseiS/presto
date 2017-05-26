package com.facebook.presto.connector.thrift.api.datatypes;

final class TypeUtils
{
    private TypeUtils()
    {
    }

    public static boolean sameSizeIfPresent(boolean[] nulls, int[] sizes)
    {
        return nulls == null || sizes == null || nulls.length == sizes.length;
    }

    public static int totalSize(boolean[] nulls, int[] sizes)
    {
        int numberOfRecords = nulls != null ? nulls.length : sizes != null ? sizes.length : 0;
        int total = 0;
        for (int i = 0; i < numberOfRecords; i++) {
            if (nulls == null || !nulls[i]) {
                total += sizes[i];
            }
        }
        return total;
    }

    public static int[] calculateOffsets(int[] sizes, boolean[] nulls, int totalRecords)
    {
        if (sizes == null) {
            return new int[totalRecords + 1];
        }
        int[] offsets = new int[totalRecords + 1];
        offsets[0] = 0;
        for (int i = 0; i < totalRecords; i++) {
            int size = nulls != null && nulls[i] ? 0 : sizes[i];
            offsets[i + 1] = offsets[i] + size;
        }
        return offsets;
    }
}
