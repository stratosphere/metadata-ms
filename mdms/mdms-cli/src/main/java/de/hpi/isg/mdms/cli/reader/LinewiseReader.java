package de.hpi.isg.mdms.cli.reader;

/**
 * A supplier of lines.
 */
public interface LinewiseReader {

    class ReadException extends Exception {

        public ReadException(String message, Throwable cause) {
            super(message, cause);
        }

        public ReadException(Throwable cause) {
            super(cause);
        }
    }


    String readLine() throws ReadException;

}
