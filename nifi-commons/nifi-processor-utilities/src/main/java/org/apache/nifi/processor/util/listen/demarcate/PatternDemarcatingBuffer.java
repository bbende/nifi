package org.apache.nifi.processor.util.listen.demarcate;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * A DemarcatingBuffer that identifies events based on a regular expression.
 */
public class PatternDemarcatingBuffer extends AbstractDemarcatingBuffer {

    private final Charset charset;
    private final Pattern pattern;
    private final DemarcatingBuffer lineDemarcatingBuffer;

    public PatternDemarcatingBuffer(final String pattern, final Charset charset) {
        this(DEFAULT_BUFFER_SIZE, pattern, charset);
    }

    public PatternDemarcatingBuffer(final int initialSize, final String pattern, final Charset charset) {
        super(initialSize);
        this.charset = charset;
        this.pattern = Pattern.compile(pattern);
        this.lineDemarcatingBuffer = new LiteralDemarcatingBuffer(initialSize, new byte[] { '\n' });
    }

    @Override
    public byte[] add(byte b) {
        byte[] event = null;
        final byte[] line = lineDemarcatingBuffer.add(b);

        if (line != null) {
            final String lineStr = new String(line, charset);

            // if latest line matches the pattern, then everything before it is an event
            if (pattern.matcher(lineStr).matches()) {
                event = buffer.toByteArray();
                buffer.reset();
            }

            // always write the current line and the demarcator to the overall buffer
            buffer.write(line, 0, line.length);
            buffer.write(b);
        }

        // returns null if we aren't at the end of a line, or if the pattern didn't match the current line
        // otherwise returns all of the bytes buffered prior to the current line
        return event;
    }

}
