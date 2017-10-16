package de.hpi.isg.mdms.metanome.friendly;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;

import java.util.regex.Pattern;

/**
 * Parser for {@link UniqueColumnCombination} constraints.
 */
public class UniqueColumnCombinationReader extends AbstractFriendlyReader<UniqueColumnCombination> {

    @Override
    protected void processLine(String line, DependencyResultReceiver<?> resultReceiver) {
        try {
            resultReceiver.receiveResult(toUCC(line));
        } catch (CouldNotReceiveResultException e) {
            throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
        }
    }

    private static UniqueColumnCombination toUCC(String line) {
        String cleanLine = line.replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), "");
        return new UniqueColumnCombination(toColumnCombination(cleanLine));
    }
}
