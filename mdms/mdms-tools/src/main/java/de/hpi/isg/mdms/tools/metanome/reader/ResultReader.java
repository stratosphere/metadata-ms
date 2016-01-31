package de.hpi.isg.mdms.tools.metanome.reader;

import de.metanome.backend.result_receiver.ResultReceiver;

import java.io.File;
import java.io.IOException;

/**
 * Parses a Metanome result file.
 */
public interface ResultReader {

    /**
     * Parsers a given result file and hands the results to the given {@core resultReceiver}
     * @param resultFile a Metanome result file
     * @param resultReceiver consumer for the parsed results
     * @throws IOException if the {@code resultFile} cannot be read properly or the parsing fails
     */
    void parse(File resultFile, ResultReceiver resultReceiver);

}
