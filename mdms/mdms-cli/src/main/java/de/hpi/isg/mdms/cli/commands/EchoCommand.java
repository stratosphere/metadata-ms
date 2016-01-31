package de.hpi.isg.mdms.cli.commands;

import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

/**
 * Command to echo an input parameter.
 */
public class EchoCommand implements Command {
    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("echo");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx) throws CliException {
        final ContextObject argument = commandLine.getArguments().size() >= 1 ?
                commandLine.getArguments().get(0) : ContextObject.NULL;
        if (argument != ContextObject.NULL) {
            printer.println(argument.toReadableString());
        }
        return argument;
    }

    @Override
    public String getShortDescription() {
        return "Returns the first given argument.";
    }

    @Override
    public String getUsage() {
        return "echo [<anything>]";
    }
}
