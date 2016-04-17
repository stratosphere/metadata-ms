package de.hpi.isg.mdms.flink.functions;

import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.flink.util.CsvParser;
import de.hpi.isg.mdms.util.ArrayHeap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Parses a CSV row into a {@link Tuple}.
 * 
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ForwardedFields("0 -> minColumnId")
public class ParseCsvRows implements MapFunction<Tuple2<Integer, String>, Tuple> {

    private static final long serialVersionUID = 1377116120504051734L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ParseCsvRows.class);

    private final Tuple outputTuple = new Tuple();

    private final CsvParser parser;

    private final ArrayHeap<String> arrayHeap = new ArrayHeap<>(String.class);

    public ParseCsvRows(final char fieldSeparator, final char quoteChar, String nullString) {
        this.parser = new CsvParser(fieldSeparator, quoteChar, nullString);
    }

    @Override
    public Tuple map(final Tuple2<Integer, String> fileLine)
            throws Exception {

        final List<String> fields = this.parser.parse(fileLine.f1);
        final String[] fieldArray = this.arrayHeap.yield(fields.size());
        fields.toArray(fieldArray);

        this.outputTuple.setMinColumnId(fileLine.f0);
        this.outputTuple.setFields(fieldArray);
        return this.outputTuple;

    }
}
