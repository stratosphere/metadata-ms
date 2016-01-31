package de.hpi.isg.mdms.cli.commands;


import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;
import de.hpi.isg.mdms.cli.variables.ContextReference;
import de.hpi.isg.mdms.cli.variables.ContextValue;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

/**
 * This {@link Command} places a value in the global namespace.
 */
public class SetCommand implements Command {

    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("set");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx) throws CliException {
        final ContextReference targetRef = commandLine.getArgument(0, ContextReference.class);
        final ContextValue sourceValue = commandLine.getArgument(1, ContextValue.class);
        return ctx.getGlobalNamespace().set(targetRef, sourceValue);
    }

    @Override
    public String getShortDescription() {
        return "Places a value in the global namespace.";
    }

    @Override
    public String getUsage() {
        return "set <target reference> <value>";
    }
}
