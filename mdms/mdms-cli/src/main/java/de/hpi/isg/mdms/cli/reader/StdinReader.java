package de.hpi.isg.mdms.cli.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * This utility class facilitates the access to the standard input of the containing process. Please consider
 * that, once this class is put into use, direct access to the standard input might miss some data due to buffering.
 */
public class StdinReader implements LinewiseReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(StdinReader.class);

    private static StdinReader INSTANCE = null;

    private final BufferedReader reader;

    public static synchronized StdinReader getOrCreateInstance() {
        if (INSTANCE == null) {
            INSTANCE = new StdinReader();
        }
        return INSTANCE;
    }

    private StdinReader() {
        LOGGER.debug("Creating a STDIN reader.");
        this.reader = new BufferedReader(new InputStreamReader(System.in));
    }

    @Override
    public String readLine() throws ReadException {
        try {
            return this.reader.readLine();
        } catch (IOException e) {
            throw new ReadException("Could not read from stdin.", e);
        }
    }

}
