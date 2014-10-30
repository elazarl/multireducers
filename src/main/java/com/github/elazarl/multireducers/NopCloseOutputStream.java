package com.github.elazarl.multireducers;

import org.apache.commons.io.output.ProxyOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An outputstream that proxies all methods to the delegate outputstream
 * except of close.
 */
public class NopCloseOutputStream extends ProxyOutputStream {
    /**
     * Constructs a new NopCloseOutputStream. Identical to the
     * delegated output stream, except close() does nothing.
     *
     * @param proxy the OutputStream to delegate to
     */
    public NopCloseOutputStream(OutputStream proxy) {
        super(proxy);
    }

    @Override
    public void close() throws IOException {}
}
