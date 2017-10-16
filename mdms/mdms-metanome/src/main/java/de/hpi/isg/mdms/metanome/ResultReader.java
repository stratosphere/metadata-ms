package de.hpi.isg.mdms.metanome;

import de.metanome.backend.result_receiver.ResultReceiver;

import java.io.File;
import java.io.IOException;

/**
 * Implementing objects are able to parse some kind of Metanome result files.
 */
public interface ResultReader {

    /**
     * Read the {@code File} and feed its contents to the {@link ResultReceiver}.
     *
     * @param file           the file to be parsed
     * @param resultReceiver that receives the parsed dependencies
     * @throws IOException if the {@code file} cannot be read properly or the parsing fails
     */
    void readAndLoad(File file, DependencyResultReceiver<?> resultReceiver) throws IOException;

}
