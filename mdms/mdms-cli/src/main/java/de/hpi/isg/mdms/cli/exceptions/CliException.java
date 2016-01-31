package de.hpi.isg.mdms.cli.exceptions;


import de.hpi.isg.mdms.cli.variables.ContextObject;

/**
 * Exception of the CLI app.
 */
public class CliException extends Exception implements ContextObject {

    public CliException(String message) {
        super(message);
    }

    public CliException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String toParseableString() {
        return toString(); // TODO
    }

    @Override
    public String toReadableString() {
        return toParseableString();
    }

    @Override
    public boolean isValue() {
        return false;
    }

    @Override
    public boolean isReference() {
        return false;
    }

    @Override
    public boolean isException() {
        return true;
    }
}
