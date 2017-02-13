package de.hpi.isg.mdms.tools.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;

/**
 * Test suite for {@link PGPassFiles}.
 */
public class PGPassFilesTest {

    @Test
    public void shouldLoadValidPGPassFile() throws IOException {
        File pgPassFile = File.createTempFile("mdms", "pgpass");
        pgPassFile.deleteOnExit();

        Files.write(
                pgPassFile.toPath(),
                Collections.singleton("myhost:5432:mydb:user:pw"),
                StandardOpenOption.WRITE
        );
        PGPassFiles.PGPass pgpass = PGPassFiles.load(pgPassFile.getAbsolutePath());

        Assert.assertEquals("myhost", pgpass.hostname);
        Assert.assertEquals(5432, pgpass.port);
        Assert.assertEquals("mydb", pgpass.database);
        Assert.assertEquals("user", pgpass.username);
        Assert.assertEquals("pw", pgpass.password);
    }

    @Test
    public void shouldSupportPasswordsWithColon() throws IOException {
        File pgPassFile = File.createTempFile("mdms", "pgpass");
        pgPassFile.deleteOnExit();

        Files.write(
                pgPassFile.toPath(),
                Collections.singleton("myhost:5432:mydb:user:%:%:%::%"),
                StandardOpenOption.WRITE
        );
        PGPassFiles.PGPass pgpass = PGPassFiles.load(pgPassFile.getAbsolutePath());

        Assert.assertEquals("myhost", pgpass.hostname);
        Assert.assertEquals(5432, pgpass.port);
        Assert.assertEquals("mydb", pgpass.database);
        Assert.assertEquals("user", pgpass.username);
        Assert.assertEquals("%:%:%::%", pgpass.password);
    }

    @Test
    public void shouldLoadValidPGPassFileWithSecondLine() throws IOException {
        File pgPassFile = File.createTempFile("mdms", "pgpass");
        pgPassFile.deleteOnExit();

        Files.write(
                pgPassFile.toPath(),
                Arrays.asList("myhost:5432:mydb:user:pw", "rubbish"),
                StandardOpenOption.WRITE
        );
        PGPassFiles.PGPass pgpass = PGPassFiles.load(pgPassFile.getAbsolutePath());

        Assert.assertEquals("myhost", pgpass.hostname);
        Assert.assertEquals(5432, pgpass.port);
        Assert.assertEquals("mydb", pgpass.database);
        Assert.assertEquals("user", pgpass.username);
        Assert.assertEquals("pw", pgpass.password);
    }

}