package com.akamai.csi.multireducers;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nullable;

/**
 * PerReducerWritable is a base class implementing logic shared by
 * the PerMapperOutputKey/Value
 */
class PerInternalMapper implements Comparable<PerInternalMapper> {
    int targetReducer;
    Object data;

    PerInternalMapper() {}

    public PerInternalMapper(int targetReducer, Object data) {
        this.targetReducer = targetReducer;
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PerInternalMapper that = (PerInternalMapper) o;

        return Objects.equal(targetReducer, that.targetReducer) && Objects.equal(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(targetReducer, data);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("reducer", targetReducer).
                add("data", data).
                toString();
    }

    @Override
    public int compareTo(@Nullable PerInternalMapper that) {
        if (that == null) return 1; // nulls at start
        return ComparisonChain.start().
                compare(targetReducer, that.targetReducer).
                compare((WritableComparable)data, (WritableComparable)that.data).
                result();
    }
}
