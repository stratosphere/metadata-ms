package de.hpi.isg.mdms.tools.metanome.reader;

import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.util.regex.Pattern;

/**
 * Parser for {@link UniqueColumnCombination} constraints.
 */
public class UniqueColumnCombinationReader extends AbstractResultReader<UniqueColumnCombination> {

    @Override
    protected void processLine(String line, ResultReceiver resultReceiver) {
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
