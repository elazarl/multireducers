package com.github.elazarl.multireducers.example;

import com.github.elazarl.multireducers.MultiJob;
import com.github.elazarl.multireducers.MultiOutputFormat;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ExampleRunner runs both examples on a sample input file.
 */
public class ExampleRunner extends Configured implements Tool {

    public static class IntWritableInRange extends IntWritable {
        private ImmutableSet<Integer> s = ImmutableSet.of(120
                ,130
                ,180
                ,190
                ,130);
        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            verify();
        }

        private void verify() {
            if (!s.contains(get())) throw new RuntimeException("Illegal number: " + get());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            verify();
            super.write(out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "ExampleMultiRunner");
        int numReduceTasks = job.getNumReduceTasks();
        String outputBaseDir = args[1];
        MultiJob.create().
                withMapper(SelectFirstField.class, Text.class, IntWritable.class).
                withReducer(CountFirstField.class, numReduceTasks).
                withCombiner(CountFirstField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class,
                        MultiOutputFormat.outputPath(outputBaseDir + "/first")).
                addTo(job);
        MultiJob.create().
                withMapper(SelectSecondField.class, IntWritableInRange.class, IntWritable.class).
                withReducer(CountSecondField.class, numReduceTasks).
                withCombiner(CountSecondField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class,
                        MultiOutputFormat.outputPath(outputBaseDir + "/second")).
                addTo(job);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileContext fc = FileContext.getFileContext(getConf());
        fc.mkdir(new Path(outputBaseDir), FsPermission.getDefault(), true);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ExampleRunner(), args);
        System.exit(exitCode);
    }
}
