package com.akamai.csi.multireducers;

import com.google.common.collect.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.mock.MockReduceContextWrapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static com.akamai.csi.multireducers.MultiTestHelper.classList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiReducerTest {

    static Multimap<String, Integer> reduceCalled;

    @Before
    public void before() {
        reduceCalled = HashMultimap.create();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testReduce() throws Exception {
        Configuration conf = new Configuration();
        conf.set(MultiReducer.CONF_KEY, classList(FirstReducer.class, SecondReducer.class, ThirdReducer.class));
        MultiReducer multiReducer = new MultiReducer();
        Reducer.Context mockContext = new MockReduceContextWrapper(Collections.EMPTY_LIST, new Counters(), conf).getMockContext();
        multiReducer.setup(mockContext);
        multiReducer.reduce(new PerMapperOutputKey(0, new IntWritable(1)), ImmutableList.of(), mockContext);
        multiReducer.reduce(new PerMapperOutputKey(0, new IntWritable(2)), ImmutableList.of(), mockContext);
        multiReducer.reduce(new PerMapperOutputKey(0, new IntWritable(3)), ImmutableList.of(), mockContext);
        assertThat(ImmutableSet.copyOf(reduceCalled.get("first")), is(ImmutableSet.of(1, 2, 3)));
        assertThat(reduceCalled.containsKey("second"), is(false));
        assertThat(reduceCalled.containsKey("third"), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCombine() throws Exception {
        Configuration conf = new Configuration();
        conf.set(MultiCombiner.CONF_KEY, classList(FirstReducer.class, SecondReducer.class, ThirdReducer.class));
        MultiReducer multiReducer = new MultiCombiner();
        Reducer.Context mockContext = new MockReduceContextWrapper(Collections.EMPTY_LIST, new Counters(), conf).getMockContext();
        multiReducer.setup(mockContext);
        multiReducer.reduce(new PerMapperOutputKey(0, new IntWritable(1)), ImmutableList.of(), mockContext);
        multiReducer.reduce(new PerMapperOutputKey(0, new IntWritable(2)), ImmutableList.of(), mockContext);
        multiReducer.reduce(new PerMapperOutputKey(0, new IntWritable(3)), ImmutableList.of(), mockContext);
        multiReducer.reduce(new PerMapperOutputKey(2, new IntWritable(21)), ImmutableList.of(), mockContext);
        assertThat(ImmutableSet.copyOf(reduceCalled.get("first")), is(ImmutableSet.of(1, 2, 3)));
        assertThat(reduceCalled.containsKey("second"), is(false));
        assertThat(ImmutableSet.copyOf(reduceCalled.get("third")), is(ImmutableSet.of(21)));
    }

    static public class ReportReducer extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            reduceCalled.put(name, key.get());
        }

        private String name;

        public ReportReducer(String name) {
            this.name = name;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }
    }

    static public class FirstReducer extends ReportReducer {

        public FirstReducer() {
            super("first");
        }
    }
    static public class SecondReducer extends ReportReducer {

        public SecondReducer() {
            super("second");
        }
    }
    static public class ThirdReducer extends ReportReducer {

        public ThirdReducer() {
            super("third");
        }
    }
}