package org.apache.nifi.processor.util.listen.demarcate;

import org.apache.nifi.stream.io.ByteArrayOutputStream;

/**
 * Base class for DemarcatingBuffers.
 */
public abstract class AbstractDemarcatingBuffer implements DemarcatingBuffer {

    public static final int DEFAULT_BUFFER_SIZE = 4096;

    protected final ByteArrayOutputStream buffer;

    public AbstractDemarcatingBuffer(int size) {
        this.buffer = new ByteArrayOutputStream(size);
    }

}
