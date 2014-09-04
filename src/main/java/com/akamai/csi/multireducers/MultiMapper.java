package com.akamai.csi.multireducers;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * MultiMapper would run multiple mappers job, each with its own mapper.
 */
public class MultiMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, PerMapperOutputKey, PerMapperOutputValue> {

    public static final String CONF_KEY = "com.akamai.csi.multireducers.mappers";
    private List<TaskAttemptContext> contexts;

    @SuppressWarnings("unchecked")
    @Override
    protected void map(KEYIN key, VALUEIN value, final Context context) throws IOException, InterruptedException {
        for (int i = 0; i < mappers.size(); i++) {
            Methods.invoke(maps.get(i), mappers.get(i), key, value, contexts.get(i));
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        @SuppressWarnings("unchecked")
        Class<Mapper>[] mappersClass = (Class<Mapper>[]) conf.getClasses(CONF_KEY);
        mappers = new ArrayList<Mapper>(mappersClass.length);
        cleanups = new ArrayList<Method>(mappersClass.length);
        maps = new ArrayList<Method>(mappersClass.length);
        WrappedMapper wrappedMapper = new WrappedMapper();
        contexts = Lists.newArrayList();
        for (int i = 0; i < mappersClass.length; i++) {
            Class<Mapper> mapperClass = mappersClass[i];
            final int finalI = i;
            WrappedMapper.Context myContext = wrappedMapper.new Context(context) {
                @Override
                public void write(Object k, Object v) throws IOException, InterruptedException {
                    context.write(new PerMapperOutputKey(finalI, k),
                            new PerMapperOutputValue(finalI, v));
                }
            };
            contexts.add(myContext);
            Mapper mapper = ReflectionUtils.newInstance(mapperClass, conf);
            mappers.add(mapper);
            Methods.invoke(Methods.get(mapperClass, "setup", Context.class), mapper, myContext);
            cleanups.add(Methods.get(mapperClass, "cleanup", Context.class));
            maps.add(Methods.getWithNameMatches(mapperClass, "map"));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < mappers.size(); i++) {
            Methods.invoke(cleanups.get(i), mappers.get(i), contexts.get(i));
        }
    }

    List<Mapper> mappers;
    List<Method> maps;
    List<Method> cleanups;

}
