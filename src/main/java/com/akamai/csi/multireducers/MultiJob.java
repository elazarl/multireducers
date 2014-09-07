package com.akamai.csi.multireducers;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * MultiJob is a helper class that helps you configure a multiplexed job.
 */
public class MultiJob {

    public static final String MULTIREDUCERS_HAVE_OUTPUT_FORMAT = "com.akamai.csi.multireducers.have.output.format";
    public static final String OUTPUT_FORMAT_PATH = "com.akamai.csi.multireducers.outputFormatPath";
    public static final String NOPATH = "?nopath";
    public static final String DISABLE_JOB_PREFIX = "com.akamai.csi.multireducers.disable.job.";

    static public MultiJobBuilder create() {
        return new MultiJobBuilder();
    }

    static public class MultiJobBuilder {

        private MultiJobBuilder(){}
        private boolean skipVerification = false;
        private Class<? extends Mapper> mapper = Mapper.class;
        private Class<?> mapperOutputKey;
        private Class<?> mapperOutputValue;
        private Class<? extends Reducer> reducer = Reducer.class;
        private int numReducers = 0;
        private Class<? extends Reducer> combiner = Reducer.class;
        private Class<? extends Partitioner> partitioner = HashPartitioner.class;
        private Class<? extends OutputFormat> outputFormat = NullOutputFormat.class;
        private Class<?> outputFormatKey = NullWritable.class;
        private Class<?> outputFormatValue = NullWritable.class;
        private Class<? extends RawComparator> comparator = MultiComparator.NoComparator.class;
        private String outputFormatPath;

        public MultiJobBuilder skipJobVerificationCanCauseRuntimeErrorsIKnowWhatImDoing() {
            this.skipVerification = true;
            return this;
        }

        public MultiJobBuilder withMapper(Class<? extends Mapper> mapper, Class<?> outputKey, Class<?> outputValue) {
            this.mapper = mapper;
            this.mapperOutputKey = outputKey;
            this.mapperOutputValue = outputValue;
            return this;
        }

        public MultiJobBuilder withReducer(Class<? extends Reducer> reducer, int numReducers) {
            this.reducer = reducer;
            this.numReducers = numReducers;
            return this;
        }

        public MultiJobBuilder withCombiner(Class<? extends Reducer> combiner) {
            this.combiner = combiner;
            return this;
        }

        public MultiJobBuilder withPartitioner(Class<? extends Partitioner> partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public MultiJobBuilder withComparator(Class<? extends RawComparator> comparator) {
            this.comparator = comparator;
            return this;
        }

        public MultiJobBuilder withOutputFormat(Class<? extends OutputFormat> outputFormat,
                                                Class<?> outputFormatKey,
                                                Class<?> outputFormatValue,
                                                String outputPath) {
            this.outputFormat = outputFormat;
            this.outputFormatKey = outputFormatKey;
            this.outputFormatValue = outputFormatValue;
            this.outputFormatPath = outputPath;
            return this;
        }

        public MultiJobBuilder withOutputFormat(Class<? extends OutputFormat> outputFormat,
                                                Class<?> outputFormatKey,
                                                Class<?> outputFormatValue) {
            return withOutputFormat(outputFormat, outputFormatKey, outputFormatValue, NOPATH);
        }

        public boolean addTo(Job job) {
            if (job.getConfiguration().getBoolean(DISABLE_JOB_PREFIX + reducer.getName(), false)) {
                return false;
            }
            int jobIndex = job.getConfiguration().getStringCollection(MultiMapper.CONF_KEY).size();
            if (!outputFormat.equals(NullOutputFormat.class)) {
                job.getConfiguration().setBoolean(MULTIREDUCERS_HAVE_OUTPUT_FORMAT, true);
            }
            verifyJobIsSound();
            ensureJobSet(job);
            MultipleOutputs.addNamedOutput(job, namedOutputOf(reducer, jobIndex), outputFormat, outputFormatKey, outputFormatValue);
            appendTo(job, OUTPUT_FORMAT_PATH, outputFormatPath);
            appendTo(job, MultiMapper.CONF_KEY, mapper);
            appendTo(job, MultiReducer.CONF_KEY, reducer);
            appendTo(job, MultiPartitioner.NUM_REDUCERS_KEY, numReducers);
            appendTo(job, MultiCombiner.CONF_KEY, combiner);
            appendTo(job, MultiPartitioner.CONF_KEY, partitioner);
            appendTo(job, MultiReducer.INPUT_KEY_CLASSES, mapperOutputKey);
            appendTo(job, MultiReducer.INPUT_VALUE_CLASSES, mapperOutputValue);
            appendTo(job, MultiComparator.CONF_KEY, comparator);
            return true;
        }

        // return exception, or null if all is well
        private RuntimeException reduceMethodNotSoundError(String who, Class<?> clazz, Method reduce) {
            Class<?> key = reduce.getParameterTypes()[0];
            if (!key.isAssignableFrom(mapperOutputKey)) {
                return new IllegalArgumentException("Map output key " + mapperOutputKey.getName() +
                        ", but " + who + " " + clazz.getName() + " expects " + key.getName() + " and it cannot be assigned");
            }
            Type reduceIterator = reduce.getGenericParameterTypes()[1];
            Class<?> iteratorParameter = getTypeParameter(reduceIterator);
            if (!iteratorParameter.isAssignableFrom(mapperOutputValue)) {
                return new IllegalArgumentException("Map output value " + mapperOutputValue.getName() +
                        ", but " + who + " " + clazz.getName() + " expects " + iteratorParameter.getName() +
                        " and it cannot be assigned");
            }
            return null;
        }

        private void verifyJobIsSound() {
            if (skipVerification) {
                return;
            }
            verifyReduceMethodIsSound("reducer", reducer);
            verifyReduceMethodIsSound("combiner", combiner);
        }

        private void verifyReduceMethodIsSound(String who, Class<? extends Reducer> clazz) {
            RuntimeException error = null;
            for (Class<?> c = clazz; c != Reducer.class; c = c.getSuperclass()) {
                List<Method> methods = Methods.getAllWithName(c, "reduce");
                removeIrrelevantReduceMethods(methods);
                if (methods.isEmpty()) {
                    // Our reducer does not implement methods other than
                    // reduce(Object, Iterable, Context).
                    // This means you inherit from Reduce without type parameters.
                    // hence, the reducer can accept any type, and is always valid.
                    continue;
                }
                for (Method reduceMethod : methods) {
                    // we found a method with reduce signature
                    error = reduceMethodNotSoundError(who, clazz, reduceMethod);
                    if (error == null) {
                        return;
                    }
                }
            }
            if (error != null) {
                throw error;
            }
        }

        // for type like Iterator<Foo> returns Foo class
        private Class<?> getTypeParameter(Type type) {
            if (!(type instanceof ParameterizedType)) {
                return null;
            }
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type innerType = parameterizedType.getActualTypeArguments()[0];
            if (!(innerType instanceof Class)) {
                return null;
            }
            return (Class<?>)innerType;
        }



        /**
         * removeIrrelevantReduceMethods removes:
         * 1. methods not of the form reduce(K, Iterator<V>, Reducer.Context)
         * from the list.
         * 2. Methods of the form reduce(Object, Iterator, Reducer.Context),
         * which are generated by type erasure.
         * @param methods list of methods to remove irrelevant reduce methods from
         */
        private void removeIrrelevantReduceMethods(List<Method> methods) {
            Iterator<Method> it = methods.iterator();
            while (it.hasNext()) {
                Method method = it.next();
                Class<?>[] params = method.getParameterTypes();
                if (params.length != 3 ||
                        !params[1].equals(Iterable.class) ||
                        !params[2].isAssignableFrom(Reducer.Context.class)) {
                    // user added a reduce class that does not override parent
                    it.remove();
                } else if (method.getParameterTypes()[0].equals(Object.class) &&
                        method.getGenericParameterTypes()[1] instanceof Class) {
                    // reduce class from type erasure
                    // class Foo extends Reducer<Text, Text, Text, Text>{}
                    // would have two methods:
                    //   reduce(Text, Iterable<Text>, Context)
                    // and
                    //   reduce(Object, Iterable, Context)
                    // which is added due to type erasure. This method always accepts
                    // any input, hence irrelevant for validity check.
                    it.remove();
                }
            }
        }

    }

    public static String namedOutputOf(Class<? extends Reducer> reducer, int i) {
        return reducer.getSimpleName() + "0" + i;
    }

    private static void ensureJobSet(Job job) {
        if (job.getConfiguration().getBoolean(MULTIREDUCERS_HAVE_OUTPUT_FORMAT, false)) {
            // we need to use the TextOutputFormat, since otherwise the FileOutputCommitter won't run
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        } else {
            job.setOutputFormatClass(NullOutputFormat.class);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(MultiReducer.class);
        job.setMapperClass(MultiMapper.class);
        job.setMapOutputKeyClass(PerMapperOutputKey.class);
        job.setMapOutputValueClass(PerMapperOutputValue.class);
        job.setSortComparatorClass(MultiComparator.class);
        job.setPartitionerClass(MultiPartitioner.class);
        List<Class<?>> serializations = Arrays.asList(
                job.getConfiguration().getClasses(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
        if (serializations.indexOf(MultiSerializer.class) == -1) {
            appendTo(job, CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, MultiSerializer.class);
        }
        for (Class<?> aClass : job.getConfiguration().getClasses(MultiCombiner.CONF_KEY)) {
            if (!aClass.equals(Reducer.class)) {
                job.setCombinerClass(MultiCombiner.class);
            }
        }
    }

    public static void appendTo(Job jobConf, String key, int n) {
        appendTo(jobConf, key, "" + n);
    }

    public static void appendTo(Job jobConf, String key, Class<?> clazz) {
        appendTo(jobConf, key, clazz.getName());
    }
    public static void appendTo(Job jobConf, String key, String val) {
        Collection<String> src = jobConf.getConfiguration().getStringCollection(key);
        src.add(val);
        jobConf.getConfiguration().setStrings(key, src.toArray(new String[src.size()]));
    }
}
