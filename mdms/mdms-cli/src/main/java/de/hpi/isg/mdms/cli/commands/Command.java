package de.hpi.isg.mdms.cli.commands;


import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;

import java.io.PrintStream;
import java.util.Collection;

/**
 * Describes a command that can be executed on the command line.
 *
 * Created by basti on 10/14/15.
 */
public interface Command {

    Collection<String> getAliases();

    ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx)
            throws CliException;

    String getShortDescription();

    String getUsage();
}
