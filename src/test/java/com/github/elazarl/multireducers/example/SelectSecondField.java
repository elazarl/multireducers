package com.github.elazarl.multireducers.example;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Selects the second field in a comma separated tuple.
 */
public class SelectSecondField extends Mapper<LongWritable, Text, ExampleRunner.IntWritableInRange, IntWritable> {
    IntWritable one = new IntWritable(1);
    ExampleRunner.IntWritableInRange val = new ExampleRunner.IntWritableInRange();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        int ix = Bytes.indexOf(bytes, (byte) ',');
        if (ix < 0 || ix >= value.getLength()) {
            throw new RuntimeException("Illegal input line: " + value.toString());
        }
        val.set(Integer.parseInt(new String(Arrays.copyOfRange(bytes, ix+1, value.getLength()), Charsets.US_ASCII)));
        context.write(val, one);
    }
}
