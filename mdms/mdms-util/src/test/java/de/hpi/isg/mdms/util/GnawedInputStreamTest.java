package de.hpi.isg.mdms.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author sebastian.kruse
 * @since 29.04.2015
 */
public class GnawedInputStreamTest {

    @Test
    public void testWorksIfStreamIsNotGnawed() throws IOException {
        BufferedReader originalReader =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/leaves-of-grass.txt")));

        InputStream stream = getClass().getResourceAsStream("/leaves-of-grass.txt");
        byte[] buf = new byte[0];
        int read = stream.read(buf);
        GnawedInputStream gnawedInputStream = new GnawedInputStream(buf, read, stream);
        BufferedReader testReader = new BufferedReader(new InputStreamReader(gnawedInputStream));

        String originalLine;
        while ((originalLine = originalReader.readLine()) != null) {
            Assert.assertEquals(originalLine, testReader.readLine());
        }
        Assert.assertEquals(null, testReader.readLine());
    }

    @Test
    public void testWorksIfStreamIsGnawed() throws IOException {
        BufferedReader originalReader =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/leaves-of-grass.txt")));

        InputStream stream = getClass().getResourceAsStream("/leaves-of-grass.txt");
        byte[] buf = new byte[100000];
        int read = stream.read(buf, 0, 99999);
        GnawedInputStream gnawedInputStream = new GnawedInputStream(buf, read, stream);
        BufferedReader testReader = new BufferedReader(new InputStreamReader(gnawedInputStream));

        String originalLine;
        while ((originalLine = originalReader.readLine()) != null) {
            Assert.assertEquals(originalLine, testReader.readLine());
        }
        Assert.assertEquals(null, testReader.readLine());
    }

    @Test
    public void testWorksIfStreamIsCompletelyConsumed() throws IOException {
        BufferedReader originalReader =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/leaves-of-grass.txt")));

        InputStream stream = getClass().getResourceAsStream("/leaves-of-grass.txt");
        byte[] buf = new byte[10000000];
        int read = stream.read(buf);
        GnawedInputStream gnawedInputStream = new GnawedInputStream(buf, read, stream);
        BufferedReader testReader = new BufferedReader(new InputStreamReader(gnawedInputStream));

        String originalLine;
        while ((originalLine = originalReader.readLine()) != null) {
            Assert.assertEquals(originalLine, testReader.readLine());
        }
        Assert.assertEquals(null, testReader.readLine());
    }

}
