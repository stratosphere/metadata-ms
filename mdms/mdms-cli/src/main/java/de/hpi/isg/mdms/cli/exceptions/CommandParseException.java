package de.hpi.isg.mdms.cli.exceptions;

/**
 * Displays that a command could not be parsed appropriately.
 */
public class CommandParseException extends CliException {
    public CommandParseException(String message) {
        super(message);
    }

    public CommandParseException(String message, Throwable cause) {
        super(message, cause);
    }

}
