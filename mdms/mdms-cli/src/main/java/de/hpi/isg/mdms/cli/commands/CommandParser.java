package de.hpi.isg.mdms.cli.commands;

import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by basti on 10/16/15.
 */
public class CommandParser {

    private final SessionContext sessionContext;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandParser.class);

    private final LinewiseReader input;

    public CommandParser(LinewiseReader input, SessionContext ctx) {
        this.input = input;
        this.sessionContext = ctx;
    }

    /**
     * Parses a command line from the {@link LinewiseReader}, thereby replacing variables from the session context.
     *
     * @return the parsed tokens from the command line or {@code null} if the reader did not return anything or caused
     * an error
     */
    @Deprecated
    public String[] parseCommand() {
        try {
            String line = input.readLine();
            if (line == null) {
                return null;
            }
            // TODO: parse variables
            return line.trim().split("\\s+");
        } catch (LinewiseReader.ReadException e) {
            LOGGER.error("An error occurred while reading a command.", e);
            return null;
        }
    }

//    public List<Token> parseCommandLine() {
//        try {
//            String line = input.readLine();
//            if (line == null) {
//                return null;
//            }
//            // TODO: parse variables
//            return line.trim().split("\\s+");
//        } catch (ReadException e) {
//            LOGGER.error("An error occurred while reading a command.", e);
//            return null;
//        }
//    }


    public LinewiseReader getLinewiseReader() {
        return this.input;
    }

}
