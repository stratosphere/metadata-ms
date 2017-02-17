package de.hpi.isg.mdms.tools.metanome.json;

import de.hpi.isg.mdms.tools.metanome.DependencyResultReceiver;
import de.hpi.isg.mdms.tools.metanome.ResultReader;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.JsonConverter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Takes care of repeated parsing operations for human readable, i.e., friendly, files.
 */
public abstract class AbstractJsonReader<MetanomeType> implements ResultReader {

    protected final JsonConverter<MetanomeType> jsonConverter = new JsonConverter<>();

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
    protected void processLine(String line, DependencyResultReceiver<?> resultReceiver) {
        try {
            MetanomeType metanomeConstraint = this.jsonConverter.fromJsonString(line, this.getMetanomeTypeClass());
            this.feed(metanomeConstraint, resultReceiver);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Could not parse this line: \"%s\"", line), e);
        } catch (CouldNotReceiveResultException e) {
            throw new RuntimeException(String.format("Invalid dependency in \"%s\".", line), e);
        }
    }

    /**
     * Feed the {@code metanomeConstraint} to the {@code resultReceiver}.
     */
    protected abstract void feed(MetanomeType metanomeConstraint, DependencyResultReceiver<?> resultReceiver) throws CouldNotReceiveResultException;

    /**
     * @return the {@link Class} of the Metanome results to be parsed
     */
    protected abstract Class<MetanomeType> getMetanomeTypeClass();

}
