package de.hpi.isg.mdms.cli.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * This adapter allows to use a {@link Reader} as a {@link LinewiseReader}.
 */
public class LinewiseReaderAdapter implements LinewiseReader {

    private final BufferedReader bufferedReader;

    public LinewiseReaderAdapter(Reader reader) {
        if (reader instanceof BufferedReader) {
            this.bufferedReader = (BufferedReader) reader;
        } else {
            this.bufferedReader = new BufferedReader(reader);
        }
    }

    @Override
    public String readLine() throws ReadException {
        try {
            return this.bufferedReader.readLine();
        } catch (IOException e) {
            throw new ReadException("Could not read line.", e);
        }
    }
}
