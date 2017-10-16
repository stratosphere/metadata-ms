package de.hpi.isg.mdms.metanome.friendly;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.FunctionalDependency;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Parser for {@link FunctionalDependency} constraints.
 */
public class FunctionalDependencyReader extends AbstractFriendlyReader<FunctionalDependency> {

    @Override
    protected void processLine(String line, DependencyResultReceiver<?> resultReceiver) {
        this.toFD(line).forEach(fd -> {
            try {
                resultReceiver.receiveResult(fd);
            } catch (CouldNotReceiveResultException e) {
                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
            }
        });
    }

    private Stream<FunctionalDependency> toFD(String line) {
        String cleanLine = line.replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), "");
        String[] split = cleanLine.split(" --> ");
        String[] rhsSplit = split[1].split(", ");
        ColumnCombination lhs = toColumnCombination(split[0]);
        return Arrays
                .stream(rhsSplit)
                .map(rhs -> new FunctionalDependency(lhs, toColumnIdentifier(rhs)));
    }
}
