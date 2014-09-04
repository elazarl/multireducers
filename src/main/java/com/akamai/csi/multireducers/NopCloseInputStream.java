package com.akamai.csi.multireducers;

import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * NopCloseInputStream delegates all methods to inner stream, except of close
 */
public class NopCloseInputStream extends ProxyInputStream {
    /**
     * Constructs a new NopCloseInputStream.
     *
     * @param proxy the InputStream to delegate to
     */
    public NopCloseInputStream(InputStream proxy) {
        super(proxy);
    }

    @Override
    public void close() throws IOException {
    }
}
