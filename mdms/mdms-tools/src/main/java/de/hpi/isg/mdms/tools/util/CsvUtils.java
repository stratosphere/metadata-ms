package de.hpi.isg.mdms.tools.util;

import de.hpi.isg.mdms.flink.util.CsvParser;
import de.hpi.isg.mdms.flink.util.FileUtils;
import de.hpi.isg.mdms.util.IoUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * This class gathers a set of static methods that support activities related to the handling of CSV files.
 *
 * @author Sebastian Kruse
 */
public class CsvUtils {

    /**
     * Private constructor to avoid instantiation of this class.
     */
    private CsvUtils() {
    }

    /**
     * Counts the fields in the first row of the given CSV file.
     *
     * @param file           from which the fields shall be counted
     * @param fieldDelimiter that delimits the fields in the CSV file
     * @param quoteChar      the character that quotes the fields
     * @param fileSystem     is the filesystem of the file (optional)
     * @return the number of counted fields or 0 if the file is empty
     * @throws IOException
     */
    public static int countAttributes(final Path file, final char fieldDelimiter, char quoteChar, FileSystem fileSystem)
            throws IOException {

        fileSystem = FileUtils.ensureFileSystem(file, fileSystem);

        FSDataInputStream fileInputStream = null;
        try {
            fileInputStream = fileSystem.open(file);
            final BufferedReader fileReader = new BufferedReader(
                    new InputStreamReader(fileInputStream));
            final String line = fileReader.readLine();

            return countAttributes(line, fieldDelimiter, quoteChar);

        } finally {
            IoUtils.closeSafe(fileInputStream);
        }
    }

    /**
     * Counts the fields in the given CSV row.
     *
     * @param line           from which the fields shall be counted
     * @param fieldDelimiter that delimits the fields in the CSV file
     * @param quoteChar      the character that quotes the fields
     * @return the number of counted fields or 0 if the row is empty
     * @throws IOException
     */
    public static int countAttributes(String line, char fieldDelimiter, char quoteChar) {
        if (line == null) {
            return 0;
        }

        CsvParser csvParser = new CsvParser(fieldDelimiter, quoteChar, null);
        try {
            return csvParser.parse(line).size();
        } catch (Exception e) {
            throw new RuntimeException("Could not parse line.", e);
        }
    }

    /**
     * Returns the values of the first row of the given CSV file.
     * 
     * @param file
     *        from which the values shall be extracted
     * @param fieldDelimiter
     *        that delimits the fields in the CSV file
     * @param fileSystem
     *        is the filesystem of the file (optional)
     * @return the list of values of the first row of the file or an empty list if the file is empty
     * @throws IOException
     */
    public static String[] getColumnNames(final Path file,
            final char fieldDelimiter, final char quoteChar, FileSystem fileSystem)
            throws IOException {

        fileSystem = FileUtils.ensureFileSystem(file, fileSystem);

        FSDataInputStream fileInputStream = null;
        try {
            fileInputStream = fileSystem.open(file);
            final BufferedReader fileReader = new BufferedReader(
                    new InputStreamReader(fileInputStream));
            final String line = fileReader.readLine();

            if (line == null) {
                return new String[0];
            }
            String[] values =  line.split(String.valueOf(fieldDelimiter));
            for (int i=0; i < values.length; i++) {
            	values[i].trim();

            	if (values[i].charAt(0) == quoteChar && values[i].charAt(values[i].length() - 1) == quoteChar){
            		values[i] = values[i].substring(1, values[i].length() -1);
            	}

            }
            return values;

        } finally {
            IoUtils.closeSafe(fileInputStream);
        }
    }

    
    /**
     * Creates a {@link DataSource} for a CSV file. All fields in the file will be treated as {@link String} objects.
     *
     * @param file                 is the CSV file to be read
     * @param numAttributes        is the number of attributes within the file
     * @param fieldSeparator       is the character that separates the fields within the file
     * @param executionEnvironment is needed to create the {@link DataSource}
     * @return the prepared {@link DataSource}
     */
    public static DataSource<Record> createRecordDataSource(final Path file,
                                                            final int numAttributes, final char fieldSeparator,
                                                            final ExecutionEnvironment executionEnvironment) {

        final int[] fieldIndexes = new int[numAttributes];
        @SuppressWarnings("unchecked")
        final Class<StringValue>[] fieldTypes = new Class[numAttributes];
        for (int columnIndex = 0; columnIndex < numAttributes; columnIndex++) {
            fieldIndexes[columnIndex] = columnIndex;
            fieldTypes[columnIndex] = StringValue.class;
        }

        final CsvInputFormat inputFormat = new CsvInputFormat(fieldSeparator);
        inputFormat.setFields(fieldIndexes, fieldTypes);
        inputFormat.setFilePath(file);

        return executionEnvironment.createInput(inputFormat);
    }

    /**
     * Distributes the given files into chunks with each chunk having at most a certain number of attributes (or only
     * one file with more attributes).
     *
     * @param files                 are the files to distribute
     * @param attributeIndexer      stores information over the number of attributes per file
     * @param maxAttributesPerChunk is the maximum number of attributes in one chunk
     * @return the chunks
     */
    public static Collection<Collection<Path>> chunk(final Collection<Path> files,
                                                     final AttributeIndexer attributeIndexer,
                                                     final int maxAttributesPerChunk) {

        final Collection<Collection<Path>> chunks = new ArrayList<Collection<Path>>();

        // Make a copy of the input files and distribute them to chunks using a first-fit strategy.
        final List<Path> unsortedFiles = new ArrayList<Path>(files);
        while (!unsortedFiles.isEmpty()) {

            final List<Path> chunk = new ArrayList<Path>();
            int numAttributesInChunk = 0;
            for (final Iterator<Path> i = unsortedFiles.iterator(); i.hasNext(); ) {
                final Path candidate = i.next();

                // See if candidate fits into the chunk.
                final int numCandidateAttributes = attributeIndexer.getNumAttributes(candidate.getName());
                if (numAttributesInChunk == 0 ||
                        (maxAttributesPerChunk > 0
                                && numAttributesInChunk + numCandidateAttributes <= maxAttributesPerChunk)) {

                    chunk.add(candidate);
                    numAttributesInChunk += numCandidateAttributes;
                    i.remove();
                }
            }
            chunks.add(chunk);
        }
        return chunks;
    }

    /**
     * Counts the number of attributes within the chunk.
     *
     * @param chunk            contains the files with the attributes
     * @param attributeIndexer stores information of the number of attributes per file
     * @return the number of attributes within the chunk
     */
    public static int getChunkSize(final Collection<Path> chunk, final AttributeIndexer attributeIndexer) {
        int size = 0;
        for (final Path path : chunk) {
            size += attributeIndexer.getNumAttributes(path.getName());
        }
        return size;
    }

}
