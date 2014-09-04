package com.akamai.csi.multireducers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class MultiPartitionerTest {

    static public class ConstantPartitioner extends Partitioner<IntWritable, Object> {
        @Override
        public int getPartition(IntWritable key, Object o2, int numPartitions) {
            return key.get() % numPartitions;
        }
    }

    @Test
    public void testGetPartition() throws Exception {
        MultiPartitioner<Object> partitioner = new MultiPartitioner<Object>();
        Configuration conf = new Configuration();
        conf.set(MultiPartitioner.CONF_KEY, MultiTestHelper.classList(
                ConstantPartitioner.class, ConstantPartitioner.class, ConstantPartitioner.class));
        conf.set(MultiPartitioner.NUM_REDUCERS_KEY, "1,2,10");
        int totalReducers = 1+2+10;
        partitioner.setConf(conf);
        assertThat(partitioner.getPartition(new PerMapperOutputKey(0, new IntWritable(0)), null, totalReducers),
                is(0));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(0, new IntWritable(1)), null, totalReducers),
                is(0));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(1, new IntWritable(0)), null, totalReducers),
                is(1));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(1, new IntWritable(1)), null, totalReducers),
                is(2));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(1, new IntWritable(2)), null, totalReducers),
                is(1));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(2, new IntWritable(0)), null, totalReducers),
                is(3));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(2, new IntWritable(1)), null, totalReducers),
                is(4));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(2, new IntWritable(9)), null, totalReducers),
                is(12));
        assertThat(partitioner.getPartition(new PerMapperOutputKey(2, new IntWritable(10)), null, totalReducers),
                is(3));
    }
}