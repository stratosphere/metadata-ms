package de.hpi.isg.mdms.flink.functions;

import de.hpi.isg.mdms.flink.util.CsvParser;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

/**
 * This function splits a given CSV row into its fields. The given first integer field will be offset by the field
 * index.
 *
 * @author Sebastian Kruse
 */
public class SplitCsvRows extends RichFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

    private static final long serialVersionUID = 1377116120504051734L;

    private final Tuple2<Integer, String> outputTuple = new Tuple2<>();

    private final Int2IntMap numFieldsPerFile;

    private final int maxFields;

    private final String nullString;

    private final boolean isSupressingEmptyCells;

    private final CsvParser csvParser;

    /**
     * Creates a new instance without checks of the number of fields in the rows and limitation of used fields but with
     * supressing of empty fields.
     *
     * @param fieldSeparator
     *        is the character that separates fields
     * @param quoteChar
     *        is the character that is used to quote fields (although unquoted fields are allowed as well)
     * @param nullString
     *        the {@link String} representation of null values or {@code null} if none
     */
    public SplitCsvRows(final char fieldSeparator, final char quoteChar, String nullString) {
        this(fieldSeparator, quoteChar, null, -1, nullString, true);
    }

    /**
     * Creates a new instance without limitation of used fields.
     *
     * @param fieldSeparator
     *        is the character that separates fields
     * @param quoteChar
     *        is the character that is used to quote fields (although unquoted fields are allowed as well)
     * @param numFieldsPerFile
     *        is a mapping of file IDs to the number of expected fields contained within each row of the respective file
     * @param lenientPolicy
     *        describes the behavior of the parser on illegal number of fields in a row
     * @param nullString
     *        the {@link String} representation of null values or {@code null} if none
     * @param isSupressingEmptyCells
     *        tells whether null fields will be forwarded by this operator or supressed
     */
    public SplitCsvRows(final char fieldSeparator, final char quoteChar, final Int2IntMap numFieldsPerFile,
                        final int lenientPolicy, final String nullString, boolean isSupressingEmptyCells) {
        this(fieldSeparator, quoteChar, numFieldsPerFile, lenientPolicy, -1, nullString, isSupressingEmptyCells);
    }

    /**
     * Creates a new instance without limitation of used fields.
     *
     * @param fieldSeparator
     *        is the character that separates fields
     * @param quoteChar
     *        is the character that is used to quote fields (although unquoted fields are allowed as well)
     * @param numFieldsPerFile
     *        is a mapping of file IDs to the number of expected fields contained within each row of the respective file
     * @param lenientPolicy
     *        describes the behavior of the parser on illegal number of fields in a row
     * @param isSupressingEmptyCells
     *        tells whether null fields will be forwarded by this operator or supressed
     * @param nullString
     *        the {@link String} representation of null values or {@code null} if none
     * @param maxColumns
     *        is the maximum number of fields to extract from each line (the checkings still apply, though; always the
     *        first fields will be taken)
     */
    public SplitCsvRows(final char fieldSeparator, final char quoteChar, final Int2IntMap numFieldsPerFile,
                        final int lenientPolicy, final int maxColumns, final String nullString,
                        boolean isSupressingEmptyCells) {

        this.csvParser = new CsvParser(fieldSeparator, quoteChar, nullString, -1, lenientPolicy);
        this.numFieldsPerFile = numFieldsPerFile;
        this.maxFields = maxColumns;
        this.nullString = nullString;
        this.isSupressingEmptyCells = isSupressingEmptyCells && this.nullString != null;
    }

    @Override
    public void flatMap(final Tuple2<Integer, String> fileLine, final Collector<Tuple2<Integer, String>> out)
            throws Exception {

        this.outputTuple.f0 = fileLine.f0;
        final String row = fileLine.f1;

        // Configure and run the parser.
        int numRequiredFields = -1;
        if (this.numFieldsPerFile != null) {
            numRequiredFields = this.numFieldsPerFile.get(fileLine.f0.intValue());
            csvParser.setNumExpectedFields(numRequiredFields);
        }

        List<String> fields;
        try {
            fields = this.csvParser.parse(row);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format("Could not parse tuple %s.", fileLine), e);
        }

        // Forward the parsed values.
        int numFieldsToRead = (this.maxFields >= 0) ? this.maxFields : fields.size();
        for (Iterator<String> i = fields.iterator(); i.hasNext() && numFieldsToRead > 0; numFieldsToRead--) {
            String field = i.next();
            if (field == null) {
                if (this.isSupressingEmptyCells) {
                    this.outputTuple.f0++;
                    continue;
                } else {
                    field = "\1";
                }
            }
            this.outputTuple.f1 = field;
            out.collect(this.outputTuple);
            this.outputTuple.f0++;
        }
    }

}
