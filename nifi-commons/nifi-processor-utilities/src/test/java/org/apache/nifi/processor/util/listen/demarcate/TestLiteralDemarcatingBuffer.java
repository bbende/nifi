package org.apache.nifi.processor.util.listen.demarcate;

import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestLiteralDemarcatingBuffer {

    @Test
    public void testNewLineDemarcator() throws IOException {
        final int numEvents = 25;
        final byte[] demarcator = "\n".getBytes(StandardCharsets.UTF_8);
        run(demarcator, numEvents);
    }

    @Test
    public void testMultiCharDemarcator() throws IOException {
        final int numEvents = 25;
        final byte[] demarcator = "NN".getBytes(StandardCharsets.UTF_8);
        run(demarcator, numEvents);
    }

    private void run(byte[] demarcator, int numEvents) throws IOException {
        final byte[] data = createInput(numEvents, demarcator);

        DemarcatingBuffer buffer = new LiteralDemarcatingBuffer(demarcator);

        List<String> events = new ArrayList<>();
        for (int i=0; i < data.length; i++) {
            final byte[] event = buffer.add(data[i]);
            if (event != null) {
                events.add(new String(event, StandardCharsets.UTF_8));
            }
        }

        Assert.assertEquals(numEvents, events.size());

        final String demarcatorStr = new String(demarcator, StandardCharsets.UTF_8);
        for (String event : events) {
            Assert.assertFalse(event.endsWith(demarcatorStr));
        }
    }

    private byte[] createInput(int numEvents, byte[] demarcator) throws IOException {
        final ByteArrayOutputStream data = new ByteArrayOutputStream();

        for (int i=0; i < numEvents; i++) {
            final String event = "message" +i;
            data.write(event.getBytes(StandardCharsets.UTF_8));
            data.write(demarcator);
        }

        return data.toByteArray();
    }
}
