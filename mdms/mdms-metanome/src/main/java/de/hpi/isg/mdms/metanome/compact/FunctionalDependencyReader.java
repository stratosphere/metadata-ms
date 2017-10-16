package de.hpi.isg.mdms.metanome.compact;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.FunctionalDependency;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Lan Jiang
 * @since 24/02/2017
 */
public class FunctionalDependencyReader extends AbstractCompactReader<FunctionalDependency> {

    private boolean isTableMapping = false;
    private boolean isColumnMapping = false;

    Map<String, String> tableMapping = new HashMap<>();
    Map<String, String> columnMapping = new HashMap<>();

    protected static final String FD_SEPARATOR_ESC = "->";

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
            Stream<FunctionalDependency> functionalDependencyStream = getFdFromString(tableMapping, columnMapping, line);
            functionalDependencyStream.forEach(functionalDependency -> {
                try {
                    resultReceiver.receiveResult(functionalDependency);
                } catch (CouldNotReceiveResultException e) {
                    throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
                }
            });
        }
    }

    private Stream<FunctionalDependency> getFdFromString(Map<String, String> tableMapping,
                                                         Map<String, String> columnMapping, String line) {
        String[] split = line.split(FD_SEPARATOR_ESC);
        String[] rhsSplit = split[1].split(",");
        ColumnCombination lhs = toColumnCombination(tableMapping, columnMapping, split[0]);
        return Arrays
                .stream(rhsSplit)
                .map(rhs -> new FunctionalDependency(lhs, toColumnIdentifier(tableMapping, columnMapping, rhs)));
    }
}
