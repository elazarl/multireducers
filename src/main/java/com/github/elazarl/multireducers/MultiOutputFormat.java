package com.github.elazarl.multireducers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;

public class MultiOutputFormat<V> extends OutputFormat<PerReducerOutputKey, V> implements Configurable {

    private static final String MULTI_OUTPUT_FORMATS = "com.github.elazarl.multireducers.output.formats";

    private OutputFormat[] outputFormats;
    private List<Map<String, String>> outputProperties;

    public static class Property {
        private final String key;
        private final String value;

        public Property(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static Property outputPath(String path) {
        return new Property("mapred.output.dir", path);
    }

    public static Property outputBaseName(String baseName) {
        return new Property("mapreduce.output.basename", baseName);
    }

    private static Map<String, String> propertiesToMap(Property... properties) {
        Map<String, String> m = Maps.newHashMap();
        for (Property property : properties) {
            m.put(property.key, property.value);
        }
        return m;
    }

    public static void addOutputFormat(Job job,Class<? extends OutputFormat> outputFormat,
                                       Property... properties) {
        List<String> outputFormats = Lists.newArrayList(
                job.getConfiguration().getTrimmedStringCollection(MULTI_OUTPUT_FORMATS));
        outputFormats.add(outputFormat.getName());
        job.getConfiguration().setStrings(MULTI_OUTPUT_FORMATS, outputFormats.toArray(
                new String[outputFormats.size()]));
        List<String> outputProps = Lists.newArrayList(
                job.getConfiguration().getTrimmedStringCollection(MultiJob.OUTPUT_FORMAT_PROPERTIES));
        outputProps.add(MapToProperties.serialize(propertiesToMap(properties)));
        job.getConfiguration().setStrings(MultiJob.OUTPUT_FORMAT_PROPERTIES,
                outputProps.toArray(new String[outputProps.size()]));
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

    private Map<String, String> prev = Maps.newHashMap();

    private void popConfiguration(int i, JobContext context) throws IOException {
        for (String key : outputProperties.get(i).keySet()) {
            if (prev.get(key) == null) {
                context.getConfiguration().unset(key);
            } else {
                context.getConfiguration().set(key, prev.get(key));
            }
        }
        prev.clear();
    }

    private void pushConfiguration(int i, JobContext context) throws IOException {
        for (Map.Entry<String, String> entry : outputProperties.get(i).entrySet()) {
            prev.put(entry.getKey(), context.getConfiguration().get(entry.getKey()));
            context.getConfiguration().set(entry.getKey(), entry.getValue());
        }
    }

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        Class<?>[] outputFormatClasses = conf.getClasses(MULTI_OUTPUT_FORMATS);
        outputProperties = Lists.newArrayList();
        for (String serializedProperty : conf.getTrimmedStringCollection(MultiJob.OUTPUT_FORMAT_PROPERTIES)) {
            outputProperties.add(MapToProperties.deserialize(serializedProperty));
        }
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
