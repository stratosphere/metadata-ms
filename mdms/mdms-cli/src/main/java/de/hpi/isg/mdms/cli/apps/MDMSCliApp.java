package de.hpi.isg.mdms.cli.apps;

import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.commands.Command;
import de.hpi.isg.mdms.cli.commands.Commands;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.exceptions.CommandNotFoundException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.parser.CommandLineFactory;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.reader.LinewiseReaderAdapter;
import de.hpi.isg.mdms.cli.reader.StdinReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;
import de.hpi.isg.mdms.cli.variables.ContextObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;

/**
 * CLI for the MDMS.
 */
public class MDMSCliApp implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final SessionContext sessionContext = new SessionContext();

    private final CommandLineFactory commandLineFactory = new CommandLineFactory(sessionContext);

    private final LinewiseReader reader;

    private final PrintStream printer;

    private String promptIndicator = "";

    private boolean isPrintCommand;


    public static void main(String[] args) throws FileNotFoundException {
        LinewiseReader reader;
        boolean isPrintCommand;
        if (args.length > 0) {
            reader = new LinewiseReaderAdapter(new FileReader(args[0]));
            isPrintCommand = true;
        } else {
            reader = StdinReader.getOrCreateInstance();
            isPrintCommand = false;
        }
        final MDMSCliApp mdmsCli = new MDMSCliApp(reader, System.out);
        mdmsCli.setPromptIndicator("mdms> ");
        mdmsCli.setPrintCommand(isPrintCommand);
        mdmsCli.run();
    }

    public MDMSCliApp(LinewiseReader reader, PrintStream printer) {
        Commands.registerDefaultCommands(sessionContext);
        this.reader = reader;
        this.printer = printer;
    }

    @Override
    public void run() {
        CommandLine commandLine;
        this.sessionContext.revokeExitRequest();
        while (!sessionContext.isExitRequested()) {
            try {
                commandLine = readCommand();
            } catch (Exception e) {
                logger.error("Reading the command line failed.", e);
                continue;
            }
            // Pass empty lines.
            if (commandLine == null) {
                continue;
            }

            ContextObject returnValue;

            try {
                // Look up the action.
                final Command command = this.sessionContext.getCommand(commandLine.getCommand());
                if (command == null) {
                    throw new CommandNotFoundException("Unknown command: " + commandLine.getCommand());
                }

                returnValue = command.execute(commandLine, this.reader, this.printer, this.sessionContext);
            } catch (CliException e) {
                logger.error("Command execution failed.", e);
                returnValue = e;
            } catch (Throwable e) {
                logger.error("Command execution failed.", e);
                returnValue = new CliException("Command execution failed.", e);
            }

            // Sanity operation: remove null as return value.
            if (returnValue == null) {
                logger.warn("{} return null.", commandLine.getCommand());
                returnValue = ContextObjects.escapeNull(returnValue);
            }

            this.sessionContext.setReturnValue(returnValue);
            this.printer.println(returnValue);
            if (returnValue.isException()) {
                ((Exception) returnValue).printStackTrace(this.printer);
            }
        }
    }

    private CommandLine readCommand() {
        this.printer.print(this.promptIndicator);
        CommandLine commandLine = null;
        try {
            commandLine = this.commandLineFactory.readFrom(this.reader);
        } catch (LinewiseReader.ReadException e) {
            e.printStackTrace(this.printer);
        }

        if (this.isPrintCommand) {
            this.printer.println(commandLine);
        }

        return commandLine;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setPromptIndicator(String promptIndicator) {
        this.promptIndicator = promptIndicator;
    }

    public void setPrintCommand(boolean printCommand) {
        this.isPrintCommand = printCommand;
    }
}
