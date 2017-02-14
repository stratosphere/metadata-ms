package de.hpi.isg.mdms.tools.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Utility to read PGPass files.
 */
public class PGPassFiles {

    private PGPassFiles() {
    }

    /**
     * Loads a {@link PGPass} from a PGPass file.
     *
     * @param path the path to the PGPass file
     * @return the {@link PGPass}
     * @throws IOException              if reading the file failed
     * @throws IllegalArgumentException if the file is not a valid PGPass file
     */
    public static PGPass load(String path) throws IOException, IllegalArgumentException {
        String firstLine = Files.lines(new File(path).toPath()).findFirst().orElseThrow(
                () -> new IllegalArgumentException("PGPass file is empty.")
        );

        int colonIndex1 = firstLine.indexOf(':');
        int colonIndex2 = firstLine.indexOf(':', colonIndex1 + 1);
        int colonIndex3 = firstLine.indexOf(':', colonIndex2 + 1);
        int colonIndex4 = firstLine.indexOf(':', colonIndex3 + 1);

        if (colonIndex4 == -1) {
            throw new IllegalArgumentException("Could not parse the PGPass file.");
        }
        try {
            return new PGPassFiles.PGPass(
                    firstLine.substring(0, colonIndex1), // hostname
                    Integer.valueOf(firstLine.substring(colonIndex1 + 1, colonIndex2)), // port
                    firstLine.substring(colonIndex2 + 1, colonIndex3), // database
                    firstLine.substring(colonIndex3 + 1, colonIndex4), // user
                    firstLine.substring(colonIndex4 + 1) // password
            );

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Could not parse PGPass file.", e);
        }
    }

    /**
     * Provides the contents of a PGPass file.
     */
    public static final class PGPass {

        public PGPass(String hostname, int port, String database, String username, String password) {
            this.hostname = hostname;
            this.database = database;
            this.username = username;
            this.password = password;
            this.port = port;
        }

        public final String hostname, database, username, password;

        public final int port;

    }

}
