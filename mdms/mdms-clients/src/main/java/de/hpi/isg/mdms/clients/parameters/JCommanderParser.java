package de.hpi.isg.mdms.clients.parameters;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOGGER.debug("Parse command line: {}", Arrays.toString(args));
        JCommander jCommander = new JCommander(parameterObject);
        try {
            jCommander.parse(args);
        } catch (final com.beust.jcommander.ParameterException e) {
            System.err.println(e.getMessage());
            StringBuilder sb = new StringBuilder();
            jCommander.usage(sb);
            for (String line : sb.toString().split("\n")) {
                System.err.println(line);
            }
            System.exit(1);
        }
    }


}
