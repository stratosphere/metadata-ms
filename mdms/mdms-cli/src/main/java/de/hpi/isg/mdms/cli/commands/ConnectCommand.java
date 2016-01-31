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
 * Reserved but not implemented..
 */
public class ConnectCommand implements Command {
    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("connect");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx)
            throws CliException {
        return null;
    }

    @Override
    public String getShortDescription() {
        return null;
    }

    @Override
    public String getUsage() {
        return null;
    }
}
