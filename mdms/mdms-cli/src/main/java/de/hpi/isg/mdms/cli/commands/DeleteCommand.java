package de.hpi.isg.mdms.cli.commands;


import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.exceptions.CliSyntaxException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;
import de.hpi.isg.mdms.cli.variables.ContextReference;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

/**
 * Command to delete a {@link ContextObject}.
 */
public class DeleteCommand implements Command {

    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("del");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx)
            throws CliException {
        if (!commandLine.getArguments().isEmpty()) {
            throw new CliSyntaxException("Nothing to delete given.");
        }
        final ContextObject arg1 = commandLine.getArguments().get(0);
        if (!(arg1 instanceof ContextReference)) {
            throw new CliException("Not a reference: " + arg1.toParseableString());
        }
        return ctx.deleteVariable((ContextReference) arg1);
    }

    @Override
    public String getShortDescription() {
        return "Deletes a variable.";
    }

    @Override
    public String getUsage() {
        return "del [<variable>]";
    }
}
