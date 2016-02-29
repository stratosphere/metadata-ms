package de.hpi.isg.mdms.cli.commands;

import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;
import de.hpi.isg.mdms.cli.variables.ContextValue;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Command to display help.
 */
public class HelpCommand implements Command {

    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("help");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer,
                                 SessionContext ctx) throws CliException {
        if (commandLine.getArguments().isEmpty()) {
            printer.println("Available commands:");
            for (Map.Entry<String, Command> commandEntry : ctx.getCommands().entrySet()) {
                printer.format(" %-10s %s\n", commandEntry.getKey(), commandEntry.getValue().getShortDescription());
            }
        } else {
            final ContextObject args1 = commandLine.getArguments().get(0);
            if (!args1.isValue()) {
                throw new CliException("Not a value: " + args1);
            }
            String argAlias = String.valueOf(((ContextValue) args1).getValue());
            final Command command = ctx.getCommand(argAlias);
            if (command == null) {
                throw new CliException("Command not found.");
            }
            printer.println("Usage " + command.getUsage());
        }

        return null;
    }

    @Override
    public String getShortDescription() {
        return "List command aliases and shows their syntax.";
    }

    @Override
    public String getUsage() {
        return "help [<command>]";
    }
}
