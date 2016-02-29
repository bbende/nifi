package org.apache.nifi.processor.util.listen.demarcate;

import org.apache.nifi.stream.io.util.NonThreadSafeCircularBuffer;

/**
 * A DemarcatingBuffer that identifies the end of events based on a provided literal of bytes.
 */
public class LiteralDemarcatingBuffer extends AbstractDemarcatingBuffer {

    private final NonThreadSafeCircularBuffer demarcatorBuffer;

    /**
     * @param lookingFor the bytes that indicate the end of an event
     */
    public LiteralDemarcatingBuffer(final byte[] lookingFor) {
        this(DEFAULT_BUFFER_SIZE, lookingFor);
    }

    /**
     * @param initialSize the initial size of the internal buffer
     * @param lookingFor the bytes that indicate the end of an event
     */
    public LiteralDemarcatingBuffer(final int initialSize, final byte[] lookingFor) {
        super(initialSize);
        this.demarcatorBuffer = new NonThreadSafeCircularBuffer(lookingFor);
    }

    @Override
    public byte[] add(byte b) {
        boolean found = demarcatorBuffer.addAndCompare(b);

        if (found) {
            byte[] event = buffer.toByteArray();
            buffer.reset();
            return event;
        } else {
            buffer.write(b);
            return null;
        }
    }

}
