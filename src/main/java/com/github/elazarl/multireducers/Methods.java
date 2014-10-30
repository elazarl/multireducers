package com.github.elazarl.multireducers;

import com.google.common.collect.Lists;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Methods would receive a certain method either from class or from superclass.
 */
public class Methods {
    static public Method get(Class<?> clazz, String name, Class<?>... params) {
        Class<?> c = clazz;
        while (c != null) {
            try {
                Method method = c.getDeclaredMethod(name, params);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignore) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchMethodError("Cannot find method " + name + " on " + clazz + " or supertype");
    }

    static List<Method> getAllWithName(Class<?> clazz, String name) {
        List<Method> rv = Lists.newArrayList();
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals(name)) {
                rv.add(method);
            }
        }
        return rv;
    }

    static public Method getWithNameMatches(Class<?> clazz, String name) {
        Class<?> c = clazz;
        while (c != null) {
            Method[] methods = c.getDeclaredMethods();
            for (Method method : methods) {
                if (method.getName().equals(name)) {
                    method.setAccessible(true);
                    return method;
                }
            }
            c = c.getSuperclass();
        }
        throw new NoSuchMethodError("Cannot find method " + name + " on " + clazz + " or supertype");
    }

    static public Object invoke(Method method, Object cls, Object... params) {
        try {
            return method.invoke(cls, params);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

}
