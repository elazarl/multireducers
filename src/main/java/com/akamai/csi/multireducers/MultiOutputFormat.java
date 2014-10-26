package com.akamai.csi.multireducers;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MultiOutputFormat<V> extends OutputFormat<PerReducerOutputKey, V> implements Configurable {

    private static final String MULTI_OUTPUT_FORMATS = "com.akamai.csi.multireducers.output.formats";

    private OutputFormat[] outputFormats;
    private List<String> outputPaths;

    public static void addOutputFormat(Job job,Class<? extends OutputFormat> outputFormat, String outputPath) {
        List<String> outputFormats = Lists.newArrayList(
                job.getConfiguration().getTrimmedStringCollection(MULTI_OUTPUT_FORMATS));
        outputFormats.add(outputFormat.getName());
        job.getConfiguration().setStrings(MULTI_OUTPUT_FORMATS, outputFormats.toArray(
                new String[outputFormats.size()]));
        List<String> outputPaths = Lists.newArrayList(
                job.getConfiguration().getTrimmedStringCollection(MultiJob.OUTPUT_FORMAT_PATH));
        outputPaths.add(outputPath);
        job.getConfiguration().setStrings(MultiJob.OUTPUT_FORMAT_PATH,
                outputPaths.toArray(new String[outputPaths.size()]));
    }

    public static void addOutputFormat(Job job, Class<? extends OutputFormat> outputFormat) {
        addOutputFormat(job, outputFormat, MultiJob.NOPATH);
    }

    @Override
    public RecordWriter<PerReducerOutputKey, V> getRecordWriter(final TaskAttemptContext context)
            throws IOException, InterruptedException {

        final RecordWriter[] writers = new RecordWriter[outputFormats.length];
        for (int i = 0; i < outputFormats.length; i++) {
            pushConfiguration(i, context);
            writers[i] = outputFormats[i].getRecordWriter(context);
            popConfiguration(i, context);
        }

        return new RecordWriter<PerReducerOutputKey, V>() {

            @SuppressWarnings("unchecked")
            @Override
            public void write(PerReducerOutputKey key, V value) throws IOException,
                    InterruptedException {
                int i = key.targetReducer;
                pushConfiguration(i, context);
                writers[i].write(key.data, value);
                popConfiguration(i, context);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException,
                    InterruptedException {
                for (int i = 0; i < writers.length; i++) {
                    pushConfiguration(i, context);
                    writers[i].close(context);
                    popConfiguration(i, context);
                }
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
        for (int i = 0; i < outputFormats.length; i++) {
            pushConfiguration(i, context);
            outputFormats[i].checkOutputSpecs(context);
            popConfiguration(i, context);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        final OutputCommitter[] committers = new OutputCommitter[outputFormats.length];
        for (int i = 0; i < committers.length; i++) {
            pushConfiguration(i, context);
            committers[i] = outputFormats[i].getOutputCommitter(context);
            popConfiguration(i, context);
        }

        return new MultiOutputCommitter(Arrays.asList(committers));
    }

    private String prev;

    private void popConfiguration(int i, JobContext context) throws IOException {
        if (outputPaths.get(i).equals(MultiJob.NOPATH)) return;
        if (prev == null) {
            context.getConfiguration().unset("mapred.output.dir");
        } else {
            context.getConfiguration().set("mapred.output.dir", prev);
        }
    }

    private void pushConfiguration(int i, JobContext context) throws IOException {
        if (outputPaths.get(i).equals(MultiJob.NOPATH)) return;
        prev = context.getConfiguration().get("mapred.output.dir");
        context.getConfiguration().set("mapred.output.dir", outputPaths.get(i));
    }

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        Class<?>[] outputFormatClasses = conf.getClasses(MULTI_OUTPUT_FORMATS);
        outputPaths = Lists.newArrayList(conf.getTrimmedStringCollection(MultiJob.OUTPUT_FORMAT_PATH));
        outputFormats = new OutputFormat[outputFormatClasses.length];
        for (int i = 0; i < outputFormatClasses.length; i++) {
            outputFormats[i] =
                    (OutputFormat) ReflectionUtils.newInstance(outputFormatClasses[i], conf);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
