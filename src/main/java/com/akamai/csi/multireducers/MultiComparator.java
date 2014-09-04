package com.akamai.csi.multireducers;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Compares PerMapperOutputKey keys according to their defined comparator, or using
 * the default RawComparator if nothing is defined.
 */
public class MultiComparator implements RawComparator<PerInternalMapper>, Configurable{
    public static final String CONF_KEY = "com.akamai.csi.multireducers.comparators";

    private DataInputBuffer rhsBuffer = new DataInputBuffer();
    private DataInputBuffer lhsBuffer = new DataInputBuffer();
    private PerInternalMapper lhs = new PerInternalMapper(0, null);
    private PerInternalMapper rhs = new PerInternalMapper(0, null);

    public static abstract class NoComparator implements RawComparator{}

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        lhsBuffer.reset(b1, s1, l1);
        rhsBuffer.reset(b2, s2, l2);
        try {
            int lhsReducerNum = WritableUtils.readVInt(lhsBuffer);
            int rhsReducerNum = WritableUtils.readVInt(rhsBuffer);
            int vintLen = WritableUtils.getVIntSize(lhsReducerNum);
            if (lhsReducerNum != rhsReducerNum) {
                return lhsReducerNum-rhsReducerNum;
            }
            if (comparators[rhsReducerNum] != null) {
                return comparators[lhsReducerNum].compare(b1, s1+vintLen, l1-vintLen,
                        b2, s2+vintLen, l2-vintLen);
            }
            lhsBuffer.reset(b1, s1, l1);
            rhsBuffer.reset(b2, s2, l2);
            PerInternalMapper rhsPerInternalMapper = rhsDeserializer.deserialize(rhs);
            PerInternalMapper lhsPerInternalMapper = lhsDeserializer.deserialize(lhs);
            return lhsPerInternalMapper.compareTo(rhsPerInternalMapper);
        } catch (IOException e) {
            throw new RuntimeException("cannot have IOError on in memory buffer", e);
        }
    }

    @Override
    public int compare(PerInternalMapper rhs, PerInternalMapper lhs) {
        return rhs.compareTo(lhs);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        MultiSerializer multiSerializer = new MultiSerializer();
        multiSerializer.setConf(conf);
        Class<?>[] comparatorClasses = conf.getClasses(CONF_KEY);
        comparators = new RawComparator[comparatorClasses.length];
        for (int i = 0; i < comparatorClasses.length; i++) {
            if (!comparatorClasses[i].equals(NoComparator.class)) {
                comparators[i] = (RawComparator) ReflectionUtils.newInstance(comparatorClasses[i], conf);
            }
        }
        lhsDeserializer = multiSerializer.getDeserializer(PerMapperOutputKey.class);
        rhsDeserializer = multiSerializer.getDeserializer(PerMapperOutputKey.class);
        try {
            lhsDeserializer.open(lhsBuffer);
            rhsDeserializer.open(rhsBuffer);
        } catch (IOException e) {
            throw new RuntimeException("cannot have IOError on in memory buffer", e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    private Configuration conf;
    Deserializer<PerInternalMapper> lhsDeserializer;
    Deserializer<PerInternalMapper> rhsDeserializer;
    RawComparator[] comparators;
}
