package de.hpi.isg.mdms.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * This stream reunifies a stream that has already been partially consumed and where the consumed part is still known.
 *
 * @author sebastian.kruse
 * @since 29.04.2015
 */
public class GnawedInputStream extends InputStream {

    private final byte[] gnawedPart;

    private final int gnawedLength;

    private int gnawedIndex = 0;

    private final InputStream gnawedStream;

    public GnawedInputStream(byte[] gnawedPart, int gnawedLength, InputStream gnawedStream) {
        if (gnawedLength > gnawedPart.length || gnawedLength < 0) {
            throw new IllegalArgumentException("Illegal length.");
        }
        this.gnawedPart = gnawedPart;
        this.gnawedLength = gnawedLength;
        this.gnawedStream = gnawedStream;
    }

    @Override
    public int read() throws IOException {
        // Try to deliver from the gnawed part at first.
        if (this.gnawedIndex < this.gnawedLength) {
            return this.gnawedPart[this.gnawedIndex++];
        }
        return this.gnawedStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException(String.format("Buffer: %d, offset: %d, length: %d.", b.length, off, len));
        }
        // Try to deliver from the gnawed part at first.
        if (this.gnawedIndex < this.gnawedLength) {
            int gnawConsumption = Math.min(this.gnawedLength - this.gnawedIndex, len);
            System.arraycopy(this.gnawedPart, this.gnawedIndex, b, off, gnawConsumption);
            this.gnawedIndex += gnawConsumption;
            // If we were less lazy, we could try to serve the full read request now.
            return gnawConsumption;
        }

        // Deliver from the stream otherwise.
        return super.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.gnawedStream.close();
    }

    @Override
    public int available() throws IOException {
        int remainingGnawedPartSize = this.gnawedLength - this.gnawedIndex;
        return remainingGnawedPartSize > 0 ? remainingGnawedPartSize : this.gnawedStream.available();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int i) {
        throw new RuntimeException("mark() is not supported.");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("reset() is not supported.");
    }
}
