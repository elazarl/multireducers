package com.akamai.csi.multireducers.example;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * DistinctAge gets a
 */
public class SelectFirstField extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text val = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int ix = Bytes.indexOf(value.getBytes(), (byte) ',');
        if (ix < 0) {
            throw new RuntimeException("Illegal input line: " + value.toString());
        }
        val.set(value.getBytes(), 0, ix);
        context.write(val, one);
    }
}
