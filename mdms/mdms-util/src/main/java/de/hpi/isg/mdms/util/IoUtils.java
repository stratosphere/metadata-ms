package de.hpi.isg.mdms.util;

import de.hpi.isg.mdms.Encoding;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.mozilla.universalchardet.UniversalDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Optional;

/**
 * This class gathers a set of static methods that support activities related to I/O activities.
 *
 * @author Sebastian Kruse
 */
public class IoUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoUtils.class);

    private static final int MAX_CHAR_DETECTION_BUFFER_SIZE = 4096 * 4;

    /**
     * Private constructor to avoid instantiation of this class.
     */
    private IoUtils() {
    }

    /**
     * Closes the given closeable if it is not null.
     *
     * @param closeable that shall be closed
     * @throws IOException
     */
    public static void close(final Closeable closeable) throws IOException {
        if (closeable == null) {
            return;
        }

        closeable.close();
    }

    /**
     * Closes the given closeable (if it is not null) and catches potential exceptions.
     *
     * @param closeable that shall be closed
     */
    public static void closeSafe(final Closeable closeable) {
        try {
            close(closeable);
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tries to detect the encoding of a text file.
     *
     * @param inputStream                whose input should be detected
     * @param gnawedInputStreamReference passes back an {@link GnawedInputStream} with the initial state of the given
     *                                   {@code inputStream}
     * @return
     * @throws IOException
     */
    public static Optional<Encoding> detectEncoding(InputStream inputStream,
                                                    Reference<InputStream> gnawedInputStreamReference)
            throws IOException {

        BOMInputStream bomInputStream = null;
        try {
            bomInputStream = new BOMInputStream(inputStream, false, Encoding.SUPPORTED_BOMS);

            // Try to deduce the file encoding from a BOM.
            if (bomInputStream.hasBOM()) {
                if (gnawedInputStreamReference != null) {
                    gnawedInputStreamReference.set(bomInputStream);
                }
                if (bomInputStream.hasBOM(ByteOrderMark.UTF_8)) {
                    return Optional.of(new Encoding(Encoding.ByteOrderMark.UTF_8, "UTF-8"));
                } else if (bomInputStream.hasBOM(ByteOrderMark.UTF_16BE)) {
                    return Optional.of(new Encoding(Encoding.ByteOrderMark.UTF_16_BE, "UTF-16BE"));
                } else if (bomInputStream.hasBOM(ByteOrderMark.UTF_16LE)) {
                    return Optional.of(new Encoding(Encoding.ByteOrderMark.UTF_16_LE, "UTF-16LE"));
                } else if (bomInputStream.hasBOM(ByteOrderMark.UTF_32BE)) {
                    return Optional.of(new Encoding(Encoding.ByteOrderMark.UTF_32_BE, "UTF-32BE"));
                } else if (bomInputStream.hasBOM(ByteOrderMark.UTF_32LE)) {
                    return Optional.of(new Encoding(Encoding.ByteOrderMark.UTF_32_LE, "UTF-32LE"));
                } else {
                    LOGGER.warn("Unknown byte order mark {} encountered.", bomInputStream.getBOM());
                }
            }

            // Try to deduce the encoding from the file content.
            LOGGER.debug("No BOM found, trying to guess encoding from the content.");
            int bufferSize = 4096;
            byte[] buf = new byte[bufferSize];
            int bufferFill = 0;

            UniversalDetector detector = new UniversalDetector(null);
            int lastFill = bufferFill;
            int numReadBytes;
            while (bufferFill < bufferSize &&
                    !detector.isDone() &&
                    (numReadBytes = bomInputStream.read(buf, bufferFill, bufferSize - bufferFill)) != -1 &&
                    (bufferFill += numReadBytes) >= lastFill) {
                detector.handleData(buf, lastFill, bufferFill - lastFill);
                lastFill = bufferFill;

                if (!detector.isDone() && bufferFill == bufferSize && bufferSize < MAX_CHAR_DETECTION_BUFFER_SIZE) {
                    byte[] newBuf = new byte[bufferSize * 2];
                    System.arraycopy(buf, 0, newBuf, 0, bufferSize);
                    bufferSize *= 2;
                    buf = newBuf;
                }
            }
            LOGGER.debug("Encoding detection ended after {} bytes (sufficient? {}).", bufferFill, detector.isDone());
            detector.dataEnd();

            if (gnawedInputStreamReference != null) {
                gnawedInputStreamReference.set(new GnawedInputStream(buf, bufferFill, bomInputStream));
            } else {
                IoUtils.close(bomInputStream);
            }


            String detectedCharset = detector.getDetectedCharset();
            if (detectedCharset != null) {
                try {
                    return Optional.of(new Encoding(Encoding.ByteOrderMark.NONE, Charset.forName(detectedCharset)));
                } catch (UnsupportedCharsetException e) {
                    LOGGER.warn("Detected unsupported charset.", detectedCharset);
                }
            } else {
                LOGGER.warn("Could not find an appropriate charset.");
            }

            return Optional.empty();
        } catch (IOException e) {
            IoUtils.closeSafe(bomInputStream);
            throw e;
        }

    }
}
