package org.apache.nifi.processor.util.listen.demarcate;

/**
 * Determines when the end of an event has been reached.
 */
public interface DemarcatingBuffer {

    /**
     *
     * @param b the byte to add to this buffer
     *
     * @return the bytes of the buffer if the end of an event has been reached, null otherwise
     */
    byte[] add(byte b);

}
