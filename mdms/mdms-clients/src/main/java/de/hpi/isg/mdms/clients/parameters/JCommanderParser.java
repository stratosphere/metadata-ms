package de.hpi.isg.mdms.clients.parameters;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.util.Arrays;

/**
 * This utility parses parameters for classes annotated with {@link JCommander}-specific configurations.
 */
public class JCommanderParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(JCommanderParser.class);

    public static void parseCommandLine(final Object parameterObject, final String... args) throws ParameterException {
        LOGGER.debug("Parse command line: {}", Arrays.toString(args));
        JCommander jCommander = new JCommander(parameterObject);
        try {
            jCommander.parse(args);
        } catch (final com.beust.jcommander.ParameterException e) {
            throw new ParameterException("Could not parse parameters.", e);
        }
    }

    /**
     * Parses parameters into the given {@code parameterObject}. If the parsing fails, print a description of the
     * problem to {@link System#err} and exit the application with the exit code {@code 1}.
     */
    public static void parseCommandLineAndExitOnError(final Object parameterObject, final String... args) {
        if (!parseCommandLineAndPrintOnError(System.err, parameterObject, args)) {
            System.exit(1);
        }
    }

    /**
     * Tries to parse the given {@code parameterObject} and prints error messages to the given {@code printStream}.
     * @return whether the parsing was successfull
     */
    public static boolean parseCommandLineAndPrintOnError(final PrintStream printStream,
                                         final Object parameterObject,
                                         final String... args) {
        LOGGER.debug("Parse command line: {}", Arrays.toString(args));
        JCommander jCommander = new JCommander(parameterObject);
        try {
            jCommander.parse(args);
        } catch (final com.beust.jcommander.ParameterException e) {
            printStream.println(e.getMessage());
            StringBuilder sb = new StringBuilder();
            jCommander.usage(sb);
            for (String line : sb.toString().split("\n")) {
                printStream.println(line);
            }

            return false;
        }

        return true;
    }


}
