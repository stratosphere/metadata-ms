package de.hpi.isg.mdms.metanome.compact;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.HashMap;
import java.util.Map;

//import de.metanome.algorithm_integration.results.UniqueColumnCombination;

/**
 * @author Lan Jiang
 * @since 24/02/2017
 */
public class InclusionDependencyReader extends AbstractCompactReader<InclusionDependency> {

    private boolean isTableMapping = false;
    private boolean isColumnMapping = false;

    Map<String, String> tableMapping = new HashMap<>();
    Map<String, String> columnMapping = new HashMap<>();

    protected static final String IND_SEPARATOR_ESC = "\\[=";

    @Override
    protected void processLine(String line, DependencyResultReceiver<?> resultReceiver) {
        if (line.startsWith(TABLE_MARKER)) {
            isTableMapping = true;
            isColumnMapping = false;
            return;
        } else if (line.startsWith(COLUMN_MARKER)) {
            isTableMapping = false;
            isColumnMapping = true;
            return;
        } else if (line.startsWith(RESULT_MARKER)) {
            isTableMapping = false;
            isColumnMapping = false;
            return;
        }

        if (isTableMapping) {
            String[] parts = line.split(MAPPING_SEPARATOR);
            if (parts[0].endsWith(".csv")) {
                parts[0] = parts[0].substring(0, parts[0].length()-4);
            }
            tableMapping.put(parts[1],parts[0]);
        } else if (isColumnMapping) {
            String[] parts = line.split(MAPPING_SEPARATOR);
            columnMapping.put(parts[1],parts[0]);
        } else {
            try {
                resultReceiver.receiveResult(getIndFromString(tableMapping, columnMapping, line));
            } catch (CouldNotReceiveResultException e) {
                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
            }
        }
    }

    private static InclusionDependency getIndFromString(Map<String, String> tableMapping,
                                                        Map<String, String> columnMapping, String str) {
        if (str.equals(""))
            return null; // Note: This is the empty set!
        String[] split = str.split(IND_SEPARATOR_ESC);
        ColumnPermutation dependent = ColumnPermutation.fromString(tableMapping, columnMapping, split[0]);
        ColumnPermutation referenced = ColumnPermutation.fromString(tableMapping, columnMapping, split[1]);
        return new InclusionDependency(dependent, referenced);
    }
}
