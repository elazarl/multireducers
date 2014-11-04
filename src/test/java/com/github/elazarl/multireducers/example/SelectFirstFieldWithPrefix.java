package com.github.elazarl.multireducers.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Selects the first field, and adds a prefix
 */
public class SelectFirstFieldWithPrefix extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String v = value.toString().split(",")[0];

        context.write(new Text("prefix_" + v), one);
    }
}
