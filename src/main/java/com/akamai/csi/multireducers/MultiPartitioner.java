package com.akamai.csi.multireducers;

import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * MultiPartitioner would work on PerMapperOutputKey/Value, and use the relevant internal partitioner
 */
public class MultiPartitioner<T> extends Partitioner<PerMapperOutputKey, T>
    implements Configurable {

    public static final String CONF_KEY = "com.akamai.csi.multireducers.partitioners";
    public static final String NUM_REDUCERS_KEY = "com.akamai.csi.multireducers.reducers.number";

    @Override
    public int getPartition(PerMapperOutputKey perMapperOutputKey, T value, int numPartitions) {
        int n = perMapperOutputKey.targetReducer;
        int reducersOffset = 0;
        for (int i = 0; i < n; i++) {
            reducersOffset += numReducers.get(i);
        }
        // % numPartitions is needed in case we do not have enough "physical" reducers, and we run both sub-reducers
        // on the same physical reducer.
        return (reducersOffset +
                partitioners.get(n).getPartition(perMapperOutputKey.data, value, numReducers.get(n))) % numPartitions;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        Class<Partitioner>[] partitionersClass = (Class<Partitioner>[])
                conf.getClasses(CONF_KEY);
        partitioners = new ArrayList<Partitioner<Object, Object>>(partitionersClass.length);
        for (Class<Partitioner> partitionerClass : partitionersClass) {
            partitioners.add(ReflectionUtils.newInstance(partitionerClass, conf));
        }
        numReducers = Ints.asList(conf.getInts(NUM_REDUCERS_KEY));
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    Configuration conf;

    List<Partitioner<Object, Object>> partitioners;
    List<Integer> numReducers;
}
