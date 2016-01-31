package de.hpi.isg.mdms.cli.commands;


import de.hpi.isg.mdms.cli.SessionContext;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.parser.CommandLine;
import de.hpi.isg.mdms.cli.reader.LinewiseReader;
import de.hpi.isg.mdms.cli.variables.*;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import it.unimi.dsi.fastutil.chars.Char2CharMap;
import it.unimi.dsi.fastutil.chars.Char2CharOpenHashMap;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Executes a SQL query on a SQLite database.
 */
public class SqliteCommand implements Command {

    private static final Char2CharMap escapableChars = new Char2CharOpenHashMap();

    static {
        escapableChars.put('$', '$');
        escapableChars.put('\\', '\\');
        escapableChars.defaultReturnValue('\0');
    }

    @Override
    public Collection<String> getAliases() {
        return Arrays.asList("sqlite");
    }

    @Override
    public ContextObject execute(CommandLine commandLine, LinewiseReader reader, PrintStream printer, SessionContext ctx) throws CliException {
        final StringValue mdsArg = commandLine.getArgument(0, StringValue.class);
        MetadataStoreParameters mdsParams = new MetadataStoreParameters();
        mdsParams.metadataStore = mdsArg.getValue();
        final MetadataStore metadataStore = MetadataStoreUtil.loadMetadataStore(mdsParams);
        final DatabaseAccess dbAccess = ((RDBMSMetadataStore) metadataStore).getSQLInterface().getDatabaseAccess();

        StringBuilder sb = new StringBuilder();
        String line;
        while (true) {
            try {
                line = reader.readLine();
            } catch (LinewiseReader.ReadException e) {
                throw new CliException("Could not read SQL script.", e);
            }
            if (line == null || line.trim().equals("done")) {
                break;
            } else {
                line = parse(line, ctx.getGlobalNamespace());
                sb.append(line).append(" ");
            }
        }

        try {
            dbAccess.flush();
        } catch (SQLException e) {
            throw new CliException("Database operation failed.", e);
        }

        try (final ResultSet resultSet = dbAccess.query(sb.toString())) {
            // Read column names etc.
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            ContextList columns = new ContextList();
            ContextList[] columnVectors = new ContextList[columnCount];
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                Namespace column = new Namespace();
                column.register("name", new StringValue(metaData.getColumnName(columnIndex)));
                final ContextList columnVector = new ContextList();
                column.register("data", columnVector);
                columnVectors[columnIndex - 1] = columnVector;
                columns.add(column);
            }

            // Read data.
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    columnVectors[i - 1].add(ContextObjects.toContextValue(resultSet.getObject(i)));
                }
            }

            return columns;
        } catch (SQLException e) {
            throw new CliException("Database operation failed.", e);
        } finally {
            metadataStore.close();
        }
    }

    public static String parse(CharSequence str, Namespace namespace) throws CliException {
        StringBuilder sb = new StringBuilder(str.length());
        int len = str.length();
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            if (c == '\\') {
                // Handle escape characters.
                c = str.charAt(++i);
                c = escapableChars.get(c);
                if (c == '\0') {
                    throw new IllegalArgumentException("Illegal escape sequence.");
                }
                sb.append(c);

            } else if (c == '$') {
                int variableStart = i;
                // Handle variables.
                c = str.charAt(++i);
                i++;
                if (c == '{') {
                    while (str.charAt(i) != '}') {
                        i++;
                    }
                } else {
                    while (i < str.length() && !Character.isSpaceChar(str.charAt(i)) && str.charAt(i) != '"') {
                        i++;
                    }
                    i--;
                }
                CharSequence variableCode = str.subSequence(variableStart, i + 1);
                final ContextObject variable = ContextObjects.parseAndResolve(variableCode, namespace);
                sb.append(variable.toReadableString());
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @Override
    public String getShortDescription() {
        return "Executes a given SQL script. Support variables. End the script by a line containing \"done\".";
    }

    @Override
    public String getUsage() {
        return "sqlite <metadata store>";
    }
}
