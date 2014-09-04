package com.akamai.csi.multireducers.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Count all distinct numbers produced by the mapper who reads the second field.
 */
public class CountSecondField extends Reducer<ExampleRunner.IntWritableInRange, IntWritable,
        ExampleRunner.IntWritableInRange, IntWritable> {
    private IntWritable val = new IntWritable();

    @Override
    protected void reduce(ExampleRunner.IntWritableInRange key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        val.set(sum);
        context.write(key, val);
    }
}
