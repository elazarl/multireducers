package com.github.elazarl.multireducers;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.mrunit.mapreduce.MapDriver.newMapDriver;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiMapperTest {

    static class IdentityMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    static class AddOneMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(key.get()+1), new IntWritable(value.get()+1));
        }
    }

    static class ToStringMapper extends Mapper<IntWritable, IntWritable, Text, IntWritable> {
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.get()+""), value);
        }
    }

    private Job job;
    private Mapper<IntWritable, IntWritable, PerMapperOutputKey, PerMapperOutputValue> mapper;
    private MapDriver<IntWritable, IntWritable, PerMapperOutputKey, PerMapperOutputValue> mapDriver;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws IOException {
        job = new Job();
        mapper = new MultiMapper();
        mapDriver = newMapDriver(mapper).withConfiguration(job.getConfiguration());
    }

    @Test
    public void testMap() throws Exception {
        MultiJob.create().withMapper(IdentityMapper.class, IntWritable.class, IntWritable.class).addTo(job);
        MultiJob.create().withMapper(AddOneMapper.class, IntWritable.class, IntWritable.class).addTo(job);

        List<Pair<PerMapperOutputKey, PerMapperOutputValue>> output = mapDriver.withInput(new IntWritable(0), new IntWritable(10)).run();
        assertThat(ImmutableList.copyOf(output), is(ImmutableList.of(
                new Pair<PerMapperOutputKey, PerMapperOutputValue>(new PerMapperOutputKey(0, new IntWritable(0)),
                        new PerMapperOutputValue(0, new IntWritable(10))),
                new Pair<PerMapperOutputKey, PerMapperOutputValue>(new PerMapperOutputKey(1, new IntWritable(1)),
                        new PerMapperOutputValue(1, new IntWritable(11)))
        )));
    }

    @Test
    public void testMapWithDifferentKeyTypes() throws Exception {
        MultiJob.create().withMapper(IdentityMapper.class, IntWritable.class, IntWritable.class).addTo(job);
        MultiJob.create().withMapper(AddOneMapper.class, IntWritable.class, IntWritable.class).addTo(job);
        MultiJob.create().withMapper(ToStringMapper.class, Text.class, IntWritable.class).addTo(job);

        List<Pair<PerMapperOutputKey, PerMapperOutputValue>> output = mapDriver.withInput(new IntWritable(100), new IntWritable(200)).run();
        assertThat(ImmutableList.copyOf(output), is(ImmutableList.of(
                        new Pair<PerMapperOutputKey, PerMapperOutputValue>(new PerMapperOutputKey(0, new IntWritable(100)),
                                new PerMapperOutputValue(0, new IntWritable(200))),
                        new Pair<PerMapperOutputKey, PerMapperOutputValue>(new PerMapperOutputKey(1, new IntWritable(101)),
                                new PerMapperOutputValue(1, new IntWritable(201))),
                        new Pair<PerMapperOutputKey, PerMapperOutputValue>(new PerMapperOutputKey(2, new Text("100")),
                                new PerMapperOutputValue(2, new IntWritable(200))))
        ));

    }
}