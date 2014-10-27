package com.akamai.csi.multireducers;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * MapToProperties
 */
public class MapToProperties {

    private static String utf8 = Charsets.UTF_8.name();
    private static String dummy = "com.akamai.csi.multireducers.mulitoutputformat.placeholder";

    static Map<String, String> deserialize(String s) {
        Map<String, String> m = Maps.newHashMap();
        String[] keyvals = s.split(";");
        for (String keyval : keyvals) {
            String[] parts = keyval.split("=");
            String key = parts[0];
            String value = "";
            if (parts.length > 1) {
                value = parts[1];
            }
            try {
                m.put(URLDecoder.decode(key, utf8), URLDecoder.decode(value, utf8));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        m.remove(dummy);
        return m;
    }

    static String serialize(Map<String, String> m) {
        List<String> parts = Lists.newArrayList();
        // This is to ensure we never return the empty string
        parts.add(dummy + "=");
        for (Map.Entry<String, String> entry : m.entrySet()) {
            try {
                String key = URLEncoder.encode(entry.getKey(), utf8);
                String value = URLEncoder.encode(entry.getValue(), utf8);
                parts.add(key + "=" + value);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return Joiner.on(";").join(parts);
    }

}
