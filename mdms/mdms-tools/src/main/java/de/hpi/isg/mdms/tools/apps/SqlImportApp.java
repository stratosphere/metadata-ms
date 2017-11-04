package de.hpi.isg.mdms.tools.apps;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.ReferenceUtils;
import de.hpi.isg.mdms.tools.sql.SQLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This application is supposed to import schema information from SQL files.
 */
// TODO: Make the class runnable from command-line.
public class SqlImportApp {

    private static final Logger logger = LoggerFactory.getLogger(SqlImportApp.class);

    /**
     * Import the tables and columns defined in SQL files into a schema.
     *
     * @param store    that stores the {@code schema}
     * @param schema   the {@link Schema} into which the tables and columns should be imported
     * @param sqlFiles the SQL files to load from
     */
    public static void importTables(MetadataStore store,
                                    Schema schema,
                                    Collection<String> sqlFiles) {
        Map<String, List<String>> columnNamesByTableName = new HashMap<>();
        for (String sqlFile : sqlFiles) {
            columnNamesByTableName.putAll(SQLParser.parseSchema(sqlFile));
        }
        for (Map.Entry<String, List<String>> entry : columnNamesByTableName.entrySet()) {
            String tableName = entry.getKey();
            List<String> columnNames = entry.getValue();
            Table table = schema.addTable(store, tableName, null, new DefaultLocation());
            int index = 0;
            for (String columnName : columnNames) {
                table.addColumn(store, columnName, null, index++);
            }
        }
    }

    /**
     * Loads the primary keys from SQL files.
     *
     * @param schema   the {@link Schema} to which the SQL files belong
     * @param sqlFiles the SQL files to load from
     * @return the loaded primary keys, encoded as {@link UniqueColumnCombination}s
     */
    public static Collection<UniqueColumnCombination> loadPrimaryKeys(Schema schema, Collection<String> sqlFiles) {
        return sqlFiles.stream()
                .flatMap(sqlFile -> SQLParser.parsePrimaryKeys(sqlFile).stream())
                .distinct()
                .map(pk -> {
                    Table table = schema.getTableByName(pk.getTableName());
                    if (table == null) {
                        logger.error("Could not resolve table of {}.", pk);
                        return null;
                    }
                    int[] columnIds = new int[pk.getColumnNames().size()];
                    int i = 0;
                    for (String columnName : pk.getColumnNames()) {
                        Column column = table.getColumnByName(columnName);
                        if (column == null) {
                            logger.error("Could not resolve column \"{}\" from {}.", columnName, pk);
                            return null;
                        }
                        columnIds[i++] = column.getId();
                    }
                    ReferenceUtils.ensureSorted(columnIds);
                    return new UniqueColumnCombination(columnIds);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Loads the foreign keys from SQL files.
     *
     * @param schema   the {@link Schema} to which the SQL files belong
     * @param sqlFiles the SQL files to load from
     * @return the loaded foreign keys, encoded as {@link InclusionDependency}s
     */
    public static Collection<InclusionDependency> loadForeignKeys(Schema schema, Collection<String> sqlFiles) {
        return sqlFiles.stream()
                .flatMap(sqlFile -> SQLParser.parseForeignKeys(sqlFile).stream())
                .distinct()
                .map(fk -> {
                    Table depTable = schema.getTableByName(fk.getDepTableName());
                    if (depTable == null) {
                        logger.error("Could not resolve dependent table of {}.", fk);
                        return null;
                    }
                    int[] depColumnIds = new int[fk.getDepColumnNames().size()];
                    int i = 0;
                    for (String columnName : fk.getDepColumnNames()) {
                        Column column = depTable.getColumnByName(columnName);
                        if (column == null) {
                            logger.error("Could not resolve column \"{}\" from {}.", columnName, fk);
                            return null;
                        }
                        depColumnIds[i++] = column.getId();
                    }

                    Table refTable = schema.getTableByName(fk.getRefTableName());
                    if (refTable == null) {
                        logger.error("Could not resolve referenced table of {}.", fk);
                        return null;
                    }
                    int[] refColumnIds = new int[fk.getRefColumnNames().size()];
                    i = 0;
                    for (String columnName : fk.getRefColumnNames()) {
                        Column column = refTable.getColumnByName(columnName);
                        if (column == null) {
                            logger.error("Could not resolve column \"{}\" from {}.", columnName, fk);
                            return null;
                        }
                        refColumnIds[i++] = column.getId();
                    }

                    ReferenceUtils.coSort(depColumnIds, refColumnIds);
                    return new InclusionDependency(depColumnIds, refColumnIds);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

}
