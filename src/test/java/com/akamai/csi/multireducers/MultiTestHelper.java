package com.akamai.csi.multireducers;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Helper test methods
 */
public class MultiTestHelper {
    public static String classList(Class<?>... classes) {
        List<String> rv = Lists.newArrayList();
        for (Class<?> aClass : classes) {
            rv.add(aClass.getName());
        }
        return Joiner.on(",").join(rv);
    }
}
