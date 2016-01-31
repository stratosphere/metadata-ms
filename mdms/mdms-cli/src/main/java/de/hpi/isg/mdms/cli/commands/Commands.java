package de.hpi.isg.mdms.cli.commands;


import de.hpi.isg.mdms.cli.SessionContext;

import java.util.Arrays;
import java.util.Collection;

/**
 * Hub for {@link Command}s.
 */
public class Commands {

    public static void registerDefaultCommands(SessionContext ctx) {
        for (Command command : createDefaultCommands()) {
            for (String alias : command.getAliases()) {
                ctx.registerCommand(alias, command);
            }
        }
    }

    private static Collection<Command> createDefaultCommands() {
        return Arrays.asList(new ExitCommand(), new DeleteCommand(), new HelpCommand(), new EchoCommand(),
                new RunCommand(), new SetCommand(), new SqliteCommand());
    }

}
