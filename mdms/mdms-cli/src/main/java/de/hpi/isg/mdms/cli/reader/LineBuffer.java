package de.hpi.isg.mdms.cli.reader;


import de.hpi.isg.mdms.cli.reader.LinewiseReader;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Exposes a {@link Queue} of {@link String}s both as {@link Writer} and {@link LinewiseReader}.
 * <p>This utility allows to use the MDMS CLI in other applications by providing a mediator to the applications
 * textual interface.</p>
 */
@SuppressWarnings("unused")
public class LineBuffer extends Writer implements LinewiseReader {

    private final Queue<String> lineQueue = new LinkedList<>();

    public void feed(String input) {
        final String[] lines = input.split("\n");
        for (String line : lines) {
            this.lineQueue.add(line);
        }
    }

    @Override
    public String readLine() {
        return this.lineQueue.poll();
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        feed(new String(cbuf, off, len));
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    public String readAll() {
        StringBuffer sb = new StringBuffer();
        String line;
        String separator = "";
        while ((line = readLine()) != null) {
            sb.append(separator).append(line);
            separator = "\n";
        }
        return sb.toString();
    }
}
