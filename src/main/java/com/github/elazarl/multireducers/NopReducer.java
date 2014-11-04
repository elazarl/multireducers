package com.github.elazarl.multireducers;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * NopReducer is a reducer that does nothing.
 */
public class NopReducer extends Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {}
}
