package de.hpi.isg.mdms.cli.exceptions;

/**
 * Displays that a given command has not been found.
 */
public class CommandNotFoundException extends CliException {

    public CommandNotFoundException(String message) {
        super(message);
    }

    public CommandNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
