package de.hpi.isg.mdms.tools.metanome.friendly;

import de.hpi.isg.mdms.tools.metanome.DependencyResultReceiver;
import de.hpi.isg.mdms.tools.metanome.ResultReader;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Takes care of repeated parsing operations for human readable, i.e., friendly, files.
 */
public abstract class AbstractFriendlyReader<T> implements ResultReader {

    protected static ColumnCombination toColumnCombination(String line) {
        if (line.equals(""))
            return new ColumnCombination(); // Note: This is the empty set!
        String[] split = line.split(", ");
        List<ColumnIdentifier> identifiers = Arrays.stream(split).map(AbstractFriendlyReader::toColumnIdentifier).collect(Collectors.toList());
        return new ColumnCombination(identifiers.toArray(new ColumnIdentifier[0]));
    }

    protected static ColumnPermutation toColumnPermutation(String line) {
        String[] split = line.split(",");
        List<ColumnIdentifier> identifiers = Arrays.stream(split).map(AbstractFriendlyReader::toColumnIdentifier).collect(Collectors.toList());
        return new ColumnPermutation(identifiers.toArray(new ColumnIdentifier[0]));
    }

    protected static ColumnIdentifier toColumnIdentifier(String line) {
        // If there are multiple separators, we rather believe, the table/column separator is the last one.
        int separatorPos = line.lastIndexOf('.');
        return new ColumnIdentifier(line.substring(0, separatorPos), line.substring(separatorPos + 1));
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
