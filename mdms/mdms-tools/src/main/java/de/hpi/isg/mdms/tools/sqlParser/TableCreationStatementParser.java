package de.hpi.isg.mdms.tools.sqlParser;

import antlrd.SQLiteBaseListener;
import antlrd.SQLiteLexer;
import antlrd.SQLiteParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.File;
import java.util.*;

/**
 * This class can parse SQL files and specifically extract the contents of {@code CREATE TABLE} statements.
 */
public class TableCreationStatementParser {

    /**
     * Parse the given SQL file and create a {@link Map} of its tables and columns.
     *
     * @param sqlFile the path to a SQL file
     * @return a {@link Map} that associates table names to the list of their column names;
     * the table names are normalized (lower-cased)
     */
    public static Map<String, List<String>> getColumnNameMap(String sqlFile) throws Exception {

        Map<String, List<String>> columnNamesMap = new HashMap<>();

        List<String> sqlStatements = getData(sqlFile);

        for (int i = 0; i < sqlStatements.size(); i++) {

            List<String> tableName = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            SQLiteLexer lexer = new SQLiteLexer(new ANTLRInputStream(sqlStatements.get(i)));
            SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

            //avoid warnings in console
            parser.removeErrorListeners();

            ParseTree tree = parser.sql_stmt();

            // Walk the `create_table_stmt` production and listen when the parser enters the column_defs.
            ParseTreeWalker.DEFAULT.walk(new SQLiteBaseListener() {
                @Override
                public void enterCreate_table_stmt(@NotNull SQLiteParser.Create_table_stmtContext ctx) {
                    if (ctx.table_name() != null) {
                        tableName.add(ctx.table_name().getText());
                    }
                }

                @Override
                public void enterColumn_def(@NotNull SQLiteParser.Column_defContext ctx) {
                    if (ctx.column_name() != null) {
                        columnNames.add(ctx.column_name().getText());
                    }
                }
            }, tree);
            if (!tableName.isEmpty()) {
                columnNamesMap.put(tableName.get(0).toLowerCase(), columnNames);
            }
        }
        return columnNamesMap;
    }

    private static List<String> getData(String fileLocation) {
        List<String> sqlStatements = new ArrayList<>();
        try {
            Scanner infile = new Scanner(new File(fileLocation)).useDelimiter(";");
            while (infile.hasNext()) {
                sqlStatements.add(infile.next().replace("()", ""));
            }
            infile.close();
            return sqlStatements;
        } catch (Exception ex) {
            System.out.println(ex);
        }
        return sqlStatements;
    }
}