package de.hpi.isg.mdms.flink.functions;

import de.hpi.isg.mdms.flink.util.CsvParser;
import de.hpi.isg.mdms.util.ArrayHeap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This function splits a given CSV row into its fields. In contrast to {@link ParseCsvRows}, this function does not
 * expect a file ID to be attached to each line.
 * 
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ForwardedFields("0")
public class ParsePlainCsvRows implements MapFunction<String, String[]> {

    private static final long serialVersionUID = 1377116120504051734L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ParsePlainCsvRows.class);

    private final CsvParser parser;

    private final ArrayHeap<String> arrayHeap = new ArrayHeap<>(String.class);

    public ParsePlainCsvRows(final char fieldSeparator, final char quoteChar, String nullString) {
        this.parser = new CsvParser(fieldSeparator, quoteChar, nullString);
    }

    @Override
    public String[] map(final String line)
            throws Exception {

        final List<String> fields = this.parser.parse(line);
        final String[] fieldArray = this.arrayHeap.yield(fields.size());
        fields.toArray(fieldArray);

       return fieldArray;
    }
}
