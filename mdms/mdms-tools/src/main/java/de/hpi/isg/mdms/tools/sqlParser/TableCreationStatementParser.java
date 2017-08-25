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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;


public class TableCreationStatementParser {

    public HashMap<String, List<String>> getColumnNameMap(String parameter) throws Exception {

        HashMap<String, List<String>> columnNamesMap = new HashMap<>();

        List sqlStatements = getData(parameter);

        for (int i = 0; i < sqlStatements.size(); i++) {

            List<String> tableName = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            SQLiteLexer lexer = new SQLiteLexer(new ANTLRInputStream((String) (sqlStatements.get(i))));
            SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

            //avoid warnings in console
            parser.removeErrorListeners();

            ParseTree tree = parser.sql_stmt();

            // Walk the `create_table_stmt` production and listen when the parser enters the column_defs.
            ParseTreeWalker.DEFAULT.walk(new SQLiteBaseListener() {
                @Override
                public void enterCreate_table_stmt(@NotNull SQLiteParser.Create_table_stmtContext ctx){
                    if(ctx.table_name() != null){
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
            if (!tableName.isEmpty()) { columnNamesMap.put(tableName.get(0), columnNames);}
        }
        return columnNamesMap;
    }

    private static List getData(String fileLocation){
        List<String> sqlStatements = new ArrayList<>();
        try{
            Scanner infile = new Scanner(new File(fileLocation)).useDelimiter(";");
            while(infile.hasNext()){
                sqlStatements.add(infile.next().replace("()",""));
            }
            infile.close();
            return sqlStatements;
        }
        catch(Exception ex){
            System.out.println(ex);
        }
        return sqlStatements;
    }
}