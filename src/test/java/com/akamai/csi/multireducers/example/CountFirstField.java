package com.akamai.csi.multireducers.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Counts all distinct elements in the first field
 */
public class CountFirstField extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable val = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        val.set(sum);
        context.write(key, val);
    }
}
