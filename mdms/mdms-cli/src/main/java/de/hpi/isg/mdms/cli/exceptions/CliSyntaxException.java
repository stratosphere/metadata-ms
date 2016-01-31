package de.hpi.isg.mdms.cli.exceptions;

/**
 * Displays violations of the CLI's syntax.
 */
public class CliSyntaxException extends CliException {

    public CliSyntaxException(String message) {
        super(message);
    }

    public CliSyntaxException(String message, Throwable cause) {
        super(message, cause);
    }
}
