package de.hpi.isg.mdms.tools.metanome.compact;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.tools.metanome.DependencyResultReceiver;
import de.hpi.isg.mdms.tools.metanome.ResultReader;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 24/02/2017
 */
abstract public class AbstractCompactReader<T> implements ResultReader {

    public static final String TABLE_COLUMN_CONCATENATOR_ESC = "\\.";

    protected static final String TABLE_MARKER = "# TABLES";
    protected static final String COLUMN_MARKER = "# COLUMN";
    protected static final String RESULT_MARKER = "# RESULTS";

    protected static final String MAPPING_SEPARATOR = "\t";

    protected static ColumnCombination toColumnCombination(Map<String, String> tableMapping,
                                                           Map<String, String> columnMapping, String line) {
        if (line.equals(""))
            return new ColumnCombination(); // Note: This is the empty set!
        String[] split = line.split(",");
        List<ColumnIdentifier> identifiers = Arrays.stream(split).map(s -> getColumnIdentifier(tableMapping, columnMapping, s))
                .collect(Collectors.toList());
        return new ColumnCombination(identifiers.toArray(new ColumnIdentifier[0]));
    }

    protected static ColumnIdentifier toColumnIdentifier(Map<String, String> tableMapping,
                                                         Map<String, String> columnMapping, String line) {
        ColumnIdentifier identifier = getColumnIdentifier(tableMapping, columnMapping, line);
        return identifier;
    }

    // used once
    public static ColumnIdentifier getColumnIdentifier(Map<String, String> tableMapping,
                                                       Map<String, String> columnMapping, String str) {
        if (str.isEmpty()) {
            return new ColumnIdentifier();
        }
        String[] parts = columnMapping.get(str).split(TABLE_COLUMN_CONCATENATOR_ESC, 2);
        String tableKey = parts[0];
        String columnName = parts[1];
        String tableName = tableMapping.get(tableKey);

        return new ColumnIdentifier(tableName, columnName);
    }

    @Override
    public void readAndLoad(final File resultFile, final DependencyResultReceiver<?> resultReceiver) {
        if (resultFile.exists()) {
            try {
                Files.lines(resultFile.toPath()).forEach(line -> this.processLine(line, resultReceiver));
            } catch (Exception e) {
                throw new RuntimeException("Could not parse " + resultFile, e);
            }
        } else {
            throw new RuntimeException("Could not find file at " + resultFile.getAbsolutePath());
        }
    }

    /**
     * Parse a line in the result file and puts the result into the {@code resultReceiver}.
     *
     * @param line           the line to parse
     * @param resultReceiver consumes the result
     */
    protected abstract void processLine(String line, DependencyResultReceiver<?> resultReceiver);

}
