package de.hpi.isg.mdms.cli.commands;


import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

/**
 * Command to exit the CLI session.
 */
public class ExitCommand implements Command {

    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("exit");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx) {
        ctx.requestExit();
        return ContextObject.NULL;
    }

    @Override
    public String getShortDescription() {
        return "Requests to exit this session.";
    }

    @Override
    public String getUsage() {
        return "exit";
    }
}
