package de.hpi.isg.mdms.cli.parser;


import de.hpi.isg.mdms.cli.variables.ContextObject;
import org.apache.commons.lang3.Validate;

import java.util.List;

/**
 * A command line represents an issued command on the command line consisting of a concrete command and a list
 * of arguments.
 */
public class CommandLine {

    private String command;

    private List<ContextObject> arguments;

    public CommandLine(String command, List<ContextObject> arguments) {
        this.command = command;
        this.arguments = arguments;
    }

    public String getCommand() {
        return command;
    }

    public List<ContextObject> getArguments() {
        return arguments;
    }

    /**
     * Retrieves an argument from this {@link CommandLine}, thereby checking the index and the class of the requested
     * argument.
     * @param index index of the requested argument
     * @param expectedClass expected {@link Class} of the argument
     * @param <T> expected type of the argument
     * @return the argument
     * @throws RuntimeException if there is no such requested argument
     */
    public <T extends ContextObject> T getArgument(int index, Class<T> expectedClass) {
        Validate.isTrue(this.arguments.size() > index, "Not enough arguments in the command line.");
        ContextObject rawArg = this.arguments.get(index);
        if (!expectedClass.isAssignableFrom(rawArg.getClass())) {
            throw new IllegalStateException(String.format("Argument at index %d is of type %s.",
                    index, rawArg.getClass().getSimpleName()));
        }
        return (T) rawArg;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.command);
        for (ContextObject arg : this.arguments) {
            sb.append(' ').append(arg.toParseableString());
        }
        return sb.toString();
    }
}
