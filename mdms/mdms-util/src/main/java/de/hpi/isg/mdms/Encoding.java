package de.hpi.isg.mdms;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.charset.Charset;

/**
 * This class describes the encoding of a file.
 *
 * @author sebastian.kruse
 * @since 28.04.2015
 */
@SuppressWarnings("serial")
public class Encoding implements Serializable {

    public static final org.apache.commons.io.ByteOrderMark[] SUPPORTED_BOMS = {
            org.apache.commons.io.ByteOrderMark.UTF_8,
            org.apache.commons.io.ByteOrderMark.UTF_16BE,
            org.apache.commons.io.ByteOrderMark.UTF_16LE,
            org.apache.commons.io.ByteOrderMark.UTF_32BE,
            org.apache.commons.io.ByteOrderMark.UTF_32LE
    };

    /**
     * Default encoding of the system: no BOM and UTF-8.
     */
    public static final Encoding DEFAULT_ENCODING = new Encoding(ByteOrderMark.NONE, Charset.forName("UTF-8"));

    public enum ByteOrderMark {

        // NB: In order to keep old encoding ordinals valid, it is better to append here.
        NONE(null),
        UTF_8("UTF-8", (byte) 0xef, (byte) 0xbb, (byte) 0xbf),
        UTF_16_BE("UTF-16BE", (byte) 0xfe, (byte) 0xff),
        UTF_16_LE("UTF-16LE", (byte) 0xff, (byte) 0xfe),
        UTF_32_BE("UTF-32BE", (byte) 0x00, (byte) 0x00, (byte) 0xfe, (byte) 0xff),
        UTF_32_LE("UTF-32LE", (byte) 0xff, (byte) 0xfe, (byte) 0x00, (byte) 0x00);

        private byte[] bomCode;

        private Charset associatedCharset;

        private ByteOrderMark(String associatedCharset, byte... bomCode) {
            this.bomCode = bomCode;
        }

        public void skip(InputStream stream) throws IOException {
            for (int i = 0; i < this.bomCode.length; i++) {
                int inputByte = stream.read();
                if ((0xFF & this.bomCode[i]) != inputByte) {
                    throw  new IllegalStateException(String.format("Did not find expected BOM %s in stream (found %x@%d).", this, inputByte, i));
                }
            }
        }
    }

    public static Encoding fromConfigString(String configString) {
        if (configString == null || configString.isEmpty()) {
            return DEFAULT_ENCODING;
        }

        int colonPos = configString.indexOf(":");
        ByteOrderMark byteOrderMark = ByteOrderMark.values()[Integer.parseInt(configString.substring(0, colonPos))];
        Charset charset = Charset.forName(configString.substring(colonPos + 1));
        return new Encoding(byteOrderMark, charset);
    }

    private ByteOrderMark byteOrderMark;

    /** Potentially lazy-initialized to guarantee serializability. */
    private transient Charset charset;

    private String charsetName;

    public Encoding(ByteOrderMark byteOrderMark, String charsetName) {
        this(byteOrderMark, Charset.forName(charsetName));
    }

    public Encoding(ByteOrderMark byteOrderMark, Charset charset) {
        this.byteOrderMark = byteOrderMark;
        setCharset(charset);
    }

    public ByteOrderMark getByteOrderMark() {
        return byteOrderMark;
    }

    public void setByteOrderMark(ByteOrderMark byteOrderMark) {
        this.byteOrderMark = byteOrderMark;
    }

    public Charset getCharset() {
        // Lazy-initialize charset.
        if (this.charset == null && this.charsetName != null) {
            setCharset(this.charsetName);
        }
        return this.charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
        this.charsetName = charset == null ? null : charset.name();
    }

    public void setCharset(String charsetName) {
        setCharset(Charset.forName(charsetName));
    }

    /**
     * Creates a reader that is configured according to this encoding.
     * @param inputStream should be decoded correctly
     * @return a {@link Reader} that wraps the input stream
     */
    public Reader applyTo(InputStream inputStream) {
        if (this.byteOrderMark != ByteOrderMark.NONE) {
            inputStream = new BOMInputStream(inputStream, SUPPORTED_BOMS);
        }

        return new InputStreamReader(inputStream, getCharset());
    }

    @Override
    public String toString() {
        return "Encoding{" +
                "byteOrderMark=" + byteOrderMark +
                ", charset=" + getCharset() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Encoding encoding = (Encoding) o;

        if (byteOrderMark != encoding.byteOrderMark) return false;
        Charset thisCharset = getCharset();
        Charset thatCharset = encoding.getCharset();
        if (thisCharset != null ? !thisCharset.equals(thatCharset) : thatCharset != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = byteOrderMark != null ? byteOrderMark.hashCode() : 0;
        result = 31 * result + (charsetName != null ? charsetName.hashCode() : 0);
        return result;
    }

    public String toConfigString() {
        return this.byteOrderMark.ordinal() + ":" + this.charsetName;
    }
}
