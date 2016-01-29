package de.hpi.isg.mdms.flink.util;

import de.hpi.isg.mdms.Encoding;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * @author sebastian.kruse
 * @since 28.04.2015
 */
public class FileUtilsTest {

    @Test
    public void testExtractExtension() {
        Collection<Tuple2<String, String>> testEntries = new LinkedList<>();
        testEntries.add(new Tuple2<>("hdfs://xxx/", null));
        testEntries.add(new Tuple2<>("hdfs://xxx/folder/file.txt", "txt"));
        testEntries.add(new Tuple2<>("hdfs://xxx/folder/file.txt.gz", "gz"));
        testEntries.add(new Tuple2<>("hdfs://xxx/folder/file.txt.gz/", "gz"));
        for (Tuple2<String, String> testEntry : testEntries) {
            Path path = new Path(testEntry.f0);
            String extension = FileUtils.extractExtension(path);
            Assert.assertEquals(testEntry.f1, extension);
        }
    }

    @Test
    public void testOpenGzippedFile() throws URISyntaxException, IOException {
        Path path = new Path(getClass().getResource("/test.csv.gz").toURI().toString());

        try (InputStream in = FileUtils.open(path, null)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String firstLine = reader.readLine();
            Assert.assertEquals("a;1;100;1;\"John Doe\"", firstLine);
        }
        ;
    }

    @Test
    public void testOpenRegularFile() throws URISyntaxException, IOException {
        Path path = new Path(getClass().getResource("/test.csv").toURI().toString());

        try (InputStream in = FileUtils.open(path, null)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String firstLine = reader.readLine();
            Assert.assertEquals("a;1;100;1;\"John Doe\"", firstLine);
        }
        ;
    }

    @Test
    public void testCharsetDetection() throws URISyntaxException, IOException {
        Collection<Tuple3<String, Encoding.ByteOrderMark, String>> testEntries = new LinkedList<>();
        testEntries.add(new Tuple3<>("/test.utf8-bom.csv",
                Encoding.ByteOrderMark.UTF_8, "UTF-8"));
        testEntries.add(new Tuple3<>("/test.utf8.csv",
                Encoding.ByteOrderMark.NONE, "UTF-8"));
        testEntries.add(new Tuple3<>("/test.utf16le-bom.csv",
                Encoding.ByteOrderMark.UTF_16_LE, "UTF-16LE"));
        testEntries.add(new Tuple3<>("/test.utf16be-bom.csv",
                Encoding.ByteOrderMark.UTF_16_BE, "UTF-16BE"));
        // Known bug: The UTF16LE/BE encoding is not properly recognized by juniversalchardet.
        // TODO: On larger test files, this might be different.
//        testEntries.add(new Tuple3<String, Encoding.ByteOrderMark, String>("/test.utf16le.csv",
//                Encoding.ByteOrderMark.UTF_16_LE, "UTF-16LE"));
//        testEntries.add(new Tuple3<String, Encoding.ByteOrderMark, String>("/test.utf16be.csv",
//                Encoding.ByteOrderMark.UTF_16_BE, "UTF-16BE"));
        testEntries.add(new Tuple3<>("/test.ucs2le.csv",
                Encoding.ByteOrderMark.UTF_16_LE, "UTF-16LE"));
        testEntries.add(new Tuple3<>("/test.ucs2be.csv",
                Encoding.ByteOrderMark.UTF_16_BE, "UTF-16BE"));
        testEntries.add(new Tuple3<>("/test.ansi.csv",
                Encoding.ByteOrderMark.NONE, "UTF-8"));
        testEntries.add(new Tuple3<>("/empty.csv",
                Encoding.DEFAULT_ENCODING.getByteOrderMark(), Encoding.DEFAULT_ENCODING.getCharset().toString()));

        for (Tuple3<String, Encoding.ByteOrderMark, String> testEntry : testEntries) {
            Path path = new Path(getClass().getResource(testEntry.f0).toURI().toString());
            Encoding encoding = FileUtils.detectEncoding(path, null, Encoding.DEFAULT_ENCODING, null);
            Assert.assertEquals(String.format("in %s", testEntry.f0),
                    testEntry.f1 == null ? null : new Encoding(testEntry.f1, testEntry.f2), encoding);
        }
    }

    @Test
    public void testDecoding() throws URISyntaxException, IOException {
        String original = null;
        for (String file : new String[]{"/test.csv", /* "/test.utf16be.csv", "/test.utf16le.csv", */
                "/test.ucs2be.csv", "/test.utf8-bom.csv", "/test.utf8.csv", "/test.ucs2le.csv",
                "/test.ucs2be.csv", "/test.ansi.csv"}) {
            Path path = new Path(getClass().getResource(file).toURI().toString());
            Encoding encoding = FileUtils.detectEncoding(path, null, Encoding.DEFAULT_ENCODING, null);
            BufferedReader reader = new BufferedReader(encoding.applyTo(FileUtils.open(path, null)));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            String fileContent = sb.toString();

            if (original == null) {
                original = fileContent;
            } else {
                Assert.assertEquals("in " + file, original, fileContent);
            }
        }
    }
}
