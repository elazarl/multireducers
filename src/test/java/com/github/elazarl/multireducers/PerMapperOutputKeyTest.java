package com.github.elazarl.multireducers;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PerMapperOutputKeyTest {

    @Test
    public void testSort() throws Exception {
        ArrayList<PerMapperOutputKey> toSort = Lists.newArrayList(
                perMapper(0, 2),
                perMapper(0, 0),
                perMapper(1, 1));
        Collections.sort(toSort);
        assertThat(toSort, is(Lists.newArrayList(
                perMapper(0, 0),
                perMapper(0, 2),
                perMapper(1, 1)
        )));
    }

    @Test
    public void testAlreadySorted() throws Exception {
        ArrayList<PerMapperOutputKey> toSort = Lists.newArrayList(
                perMapper(0, 0),
                perMapper(1, 1));
        Collections.sort(toSort);
        assertThat(toSort, is(Lists.newArrayList(
                perMapper(0, 0),
                perMapper(1, 1)
        )));
    }



    private PerMapperOutputKey perMapper(int reducer, int data) {
        return new PerMapperOutputKey(reducer, new IntWritable(data));
    }

    @Before
    public void setup() {
        Configuration conf = new Configuration();
        conf.setClass(MultiReducer.INPUT_KEY_CLASSES, IntWritable.class, Writable.class);
    }
}