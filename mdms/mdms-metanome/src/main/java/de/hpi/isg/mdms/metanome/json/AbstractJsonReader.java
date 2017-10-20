package de.hpi.isg.mdms.metanome.json;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.hpi.isg.mdms.metanome.ResultReader;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Takes care of repeated parsing operations for human readable, i.e., friendly, files.
 */
public abstract class AbstractJsonReader<MetanomeType> implements ResultReader {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Whether this reader tolerates broken lines.
     */
    private boolean isLenient = true;

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
        RuntimeException exception = null;
        try {
            MetanomeType metanomeConstraint = this.jsonConverter.fromJsonString(line, this.getMetanomeTypeClass());
            this.feed(metanomeConstraint, resultReceiver);
        } catch (IOException e) {
            exception = new RuntimeException(String.format("Could not parse this line: \"%s\"", line), e);
        } catch (CouldNotReceiveResultException e) {
            exception = new RuntimeException(String.format("Invalid dependency in \"%s\".", line), e);
        } catch (Exception e) {
            exception = new RuntimeException(String.format("Could not process \"%s\".", line), e);
        }
        if (exception != null) {
            if (this.isLenient) this.logger.error(exception.getMessage());
            else throw exception;
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

    public boolean isLenient() {
        return isLenient;
    }

    public void setLenient(boolean lenient) {
        isLenient = lenient;
    }
}
