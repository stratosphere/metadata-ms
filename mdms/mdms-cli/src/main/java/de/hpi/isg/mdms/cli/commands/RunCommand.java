package de.hpi.isg.mdms.cli.commands;

import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.ContextObject;
import de.hpi.isg.mdms.cli.variables.ContextObjects;
import de.hpi.isg.mdms.cli.variables.Namespace;
import de.hpi.isg.mdms.cli.variables.StringValue;
import de.hpi.isg.mdms.clients.apps.AppExecutionMetadata;
import de.hpi.isg.mdms.clients.apps.AppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import org.apache.commons.lang3.Validate;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Command to run an application (see {@link AppTemplate}.
 */
public class RunCommand implements Command {
    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("run");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx) throws CliException {
        final List<ContextObject> args = commandLine.getArguments();
        printer.println("Arguments: " + args);
        Validate.isTrue(args.size() >= 2);
        Validate.isInstanceOf(StringValue.class, args.get(0));
        Validate.isInstanceOf(StringValue.class, args.get(1));

        String appClassName = ((StringValue) args.get(0)).getValue();
        String parameterString = ((StringValue) args.get(1)).getValue();
        String[] appArgs = parameterString.split("\\s+");

        try {
            // Instantiate the program.
            final AppTemplate app = createAppInstnace(appClassName, appArgs, printer);

            // Execute the program.
            app.run();

            // Collect results.
            return asContextObject(app.getExecutionMetadata());

        } catch (ClassNotFoundException e) {
            throw new CliException("Unknown class: " + appClassName, e);
        } catch (NoSuchMethodException e) {
            throw new CliException("No CLI parameter constructor.", e);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new CliException("Could not create program.", e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new CliException("Program execution failed.", e);
        }
    }

    private AppTemplate createAppInstnace(String appClassName, String[] appArgs, PrintStream printer) throws ClassNotFoundException, CliException, InstantiationException, IllegalAccessException, InvocationTargetException {
        // TODO: This is kind of hacky...
        final Class<?> appClass = Class.forName(appClassName);
        final Constructor<?> paramConstructor = findParameterConstructor(appClass);
        final Object parameters = createParameters(paramConstructor);
        initalize(parameters, appArgs, printer);
        final Object o = paramConstructor.newInstance((Object) parameters);
        Validate.isInstanceOf(AppTemplate.class, o);
        return (AppTemplate) o;
    }

    private Constructor<?> findParameterConstructor(Class<?> algorithmClass) throws CliException {
        final Constructor<?>[] constructors = algorithmClass.getConstructors();
        return Arrays.stream(constructors)
                .filter(constructor -> constructor.getParameterCount() == 1)
                .findFirst()
                .orElseThrow(() -> new CliException("Could not find a parameter constructor."));
    }

    /**
     * Create a fresh parameter object for the given app.
     */
    private Object createParameters(Constructor<?> paramConstructor) throws CliException {
        final Class<?> parameterClass = paramConstructor.getParameters()[0].getType();
        try {
            return parameterClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CliException("Could not instantiate parameter class.", e);
        }
    }

    private void initalize(Object parameters, String[] args, PrintStream printStream) throws CliException {
        if (!JCommanderParser.parseCommandLineAndPrintOnError(printStream, parameters, args)) {
            throw new CliException("Could not parse the app arguments.");
        }
    }

    /**
     * Creates a {@link ContextObject} that reflects the given {@code executionMetadata}.
     *
     * @param executionMetadata should be put into a {@link ContextObject}
     * @return the {@link ContextObject} describing the given {@code executionMetadata}
     */
    private ContextObject asContextObject(AppExecutionMetadata executionMetadata) {
        Namespace result = new Namespace();
        final Map<String, Object> customData = executionMetadata.getCustomData();
        for (Map.Entry<String, Object> customDataEntry : customData.entrySet()) {
            result.register(customDataEntry.getKey(), ContextObjects.toContextValue(customDataEntry.getValue()));
        }
        return result;
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
