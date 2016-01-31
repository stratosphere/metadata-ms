package de.hpi.isg.mdms.cli.parser;


import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.*;
import de.hpi.isg.sodap.tools.cli.parser.antlr.SodapBaseListener;
import de.hpi.isg.sodap.tools.cli.parser.antlr.SodapLexer;
import de.hpi.isg.sodap.tools.cli.parser.antlr.SodapParser;
import org.antlr.v4.runtime.*;

import java.util.LinkedList;
import java.util.List;

/**
 * Builds a {@link CommandLine} from an input {@link LinewiseReader}.
 */
public class CommandLineFactory {

    private final SessionContext sessionContext;

    public CommandLineFactory(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

    public CommandLine readFrom(LinewiseReader reader) throws LinewiseReader.ReadException {
        final String line = reader.readLine();
        if (line == null) {
            sessionContext.requestExit();
            return null;
        }
        SodapLexer lexer = new SodapLexer(new ANTLRInputStream(line));
        SodapParser parser = new SodapParser(new CommonTokenStream(lexer));

//        parser.addErrorListener(new BaseErrorListener());

        final ParseListener parseListener = new ParseListener();
        parser.addParseListener(parseListener);
        parser.commandline();

        return parseListener.createCommandLine();
    }


    private static final BaseErrorListener LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            throw new IllegalStateException("failed to parse at line " + line + " due to " + msg, e);
        }
    };

    private class ParseListener extends SodapBaseListener {

        private String commandName;

        private List<ContextObject> argList = new LinkedList<>();

        @Override
        public void exitCommand(SodapParser.CommandContext ctx) {
            this.commandName = ctx.IDENTIFIER().getText();
        }


        @Override
        public void exitReferenceArg(SodapParser.ReferenceArgContext ctx) {
            // Parse the variable.
            String referenceCode = ctx.getText();
            assert referenceCode.charAt(0) == '*';
            String[] path;
            if (referenceCode.charAt(1) == '{') {
                assert referenceCode.charAt(referenceCode.length() - 1) == '}';
                path = referenceCode.substring(2, referenceCode.length() - 1).split("\\.");
            } else {
                path = new String[] { referenceCode.substring(1) };
            }

            // Add the obtained value into the argument list.
            this.argList.add(new ContextReference(path));
        }

        @Override
        public void exitNumberArg(SodapParser.NumberArgContext ctx) {
            try {
                final IntValue intValue = IntValue.fromCode(ctx.getText());
                this.argList.add(intValue);
                return;
            } catch (CliException e) {
                // Text is not an int. Head on.
            }
            throw new RuntimeException("Not implemented yet.");
//            ContextObject arg = null;
//            final String text = ctx.getText();
//            try {
//                arg = new ContextValue<>(Integer.parseInt(text));
//            } catch (NumberFormatException e) {
//                arg = new ContextValue<>(Double.parseDouble(text));
//            }
//            this.argList.add(arg);
        }

        @Override
        public void exitVariableArg(SodapParser.VariableArgContext ctx) {
            String variableCode = ctx.getText();
            try {
                // Parse the variable.
                final ContextObject contextObject = ContextObjects.parseAndResolve(variableCode,
                        CommandLineFactory.this.sessionContext.getGlobalNamespace());

                // Add the obtained value into the argument list.
                this.argList.add(contextObject);

            } catch (CliException e) {
                throw new RuntimeException("Could not resolve " + variableCode, e);
            }
        }

        @Override
        public void exitStringArg(SodapParser.StringArgContext ctx) {
            final String text = ctx.getText();
            try {
                this.argList.add(StringValue.fromCode(text, CommandLineFactory.this.sessionContext.getGlobalNamespace()));
            } catch (CliException e) {
                throw new RuntimeException("Parsing " + text + " failed.", e);
            }
        }


        public CommandLine createCommandLine() {
            return new CommandLine(this.commandName, this.argList);
        }

    }

}
