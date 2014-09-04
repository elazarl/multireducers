package com.akamai.csi.multireducers;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;
import java.util.List;

/**
 * MultiCombiner would run the combiner on
 */
public class MultiCombiner extends MultiReducer<PerMapperOutputKey, PerMapperOutputValue> {
    public static final String CONF_KEY = "com.akamai.csi.multireducers.combiners";

    @Override
    protected String conf_key() {
        return CONF_KEY;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int n = context.getConfiguration().getStrings(conf_key()).length;
        WrappedReducer wrappedReducer = new WrappedReducer();
        for (int i = 0; i < n; i++) {
            final int finalI = i;
            contexts.add(wrappedReducer.new Context(context) {
                @Override
                public void write(Object key, Object value) throws IOException, InterruptedException {
                    super.write(new PerMapperOutputKey(finalI, key), new PerMapperOutputValue(finalI, value));
                }
            });
        }
        super.setup(context);
    }

    @Override
    protected Context getContextForReducer(Context context, int i) {
        return contexts.get(i);
    }

    private List<Context> contexts = Lists.newArrayList();
}
