package com.github.elazarl.multireducers;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * MultiReducer would receive a PerReducerWritable as key, and use
 * the relevant reducer, according to the index at the PerReducerWritable.
 */
public class MultiReducer<KEYOUT, VALUEOUT> extends Reducer<PerMapperOutputKey, PerMapperOutputValue, KEYOUT, VALUEOUT> {

    public static final String CONF_KEY = "com.github.elazarl.multireducers.reducers";
    public static final String INPUT_KEY_CLASSES = "com.github.elazarl.multireducers.reducer.input.key";
    public static final String INPUT_VALUE_CLASSES = "com.github.elazarl.multireducers.reducer.input.value";

    protected String conf_key() {
        return CONF_KEY;
    }

    @Override
    protected void reduce(PerMapperOutputKey key, Iterable<PerMapperOutputValue> values, Context context) throws IOException, InterruptedException {
        int i = key.targetReducer;
        Methods.invoke(reduces.get(i), reducers.get(i), key.data,
                Iterables.transform(values, new Function<PerMapperOutputValue, VALUEOUT>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public VALUEOUT apply(PerMapperOutputValue input) {
                        return (VALUEOUT) input.data;
                    }
                    }), getContextForReducer(context, i));
    }

    protected Context getContextForReducer(Context context, int i) {
        return contexts.get(i);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        outputPaths = Lists.newArrayList(conf.getTrimmedStringCollection(MultiJob.OUTPUT_FORMAT_PATH));
        @SuppressWarnings("unchecked")
        Class<Reducer>[] reducersClass = (Class<Reducer>[]) conf.getClasses(conf_key());
        reducers = new ArrayList<Reducer>(reducersClass.length);
        cleanups = new ArrayList<Method>(reducersClass.length);
        reduces = new ArrayList<Method>(reducersClass.length);
        contexts = new ArrayList<Reducer<PerMapperOutputKey, PerMapperOutputValue, KEYOUT, VALUEOUT>.Context>();
        if (outputPaths.isEmpty()) {
            Iterables.addAll(outputPaths, Iterables.limit(Iterables.cycle(""), reducersClass.length));
        }
        WrappedReducer wrappedReducer = new WrappedReducer();
        for (int i = 0; i < reducersClass.length; i++) {
            Class<Reducer> reducerClass = reducersClass[i];
            Reducer reducer = ReflectionUtils.newInstance(reducerClass, conf);

            final int finalI = i;
            WrappedReducer.Context myContext = wrappedReducer.new Context(context) {
                @Override
                public void write(Object key, Object value) throws IOException, InterruptedException {
                    context.write((KEYOUT) new PerReducerOutputKey(finalI, key), (VALUEOUT)value);
                }
            };
            contexts.add(myContext);
            reducers.add(reducer);
            Methods.invoke(Methods.get(reducerClass, "setup", Context.class), reducer, getContextForReducer(context, i));
            cleanups.add(Methods.get(reducerClass, "cleanup", Context.class));
            reduces.add(Methods.getWithNameMatches(reducerClass, "reduce"));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < reducers.size(); i++) {
            Methods.invoke(cleanups.get(i), reducers.get(i), getContextForReducer(context, i));
        }
    }

    private List<Reducer> reducers;
    private List<Method> reduces;
    private List<Method> cleanups;
    private List<Reducer<PerMapperOutputKey, PerMapperOutputValue, KEYOUT, VALUEOUT>.Context> contexts;
    private List<String> outputPaths;
}
