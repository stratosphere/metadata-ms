package de.hpi.isg.mdms.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class parsers lines of a CSV file (without line breaks).
 */
@SuppressWarnings("serial")
public class CsvParser implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CsvParser.class);

    /**
     * Lenient-policy value: Throw an exception when a row has too few or too much fields.
     */
    public static final int FAIL_ON_ILLEGAL_LINES = 0;

    /**
     * Lenient-policy value: Throw an exception when a row has too much fields and warn if it has too few fields.
     */
    public static final int FAIL_ON_OVERLONG_LINES = 1;

    /**
     * Lenient-policy value: Warn when a row has too few or too much fields.
     */
    public static final int WARN_ON_ILLEGAL_LINES = 2;

    private final char fieldSeparator;

    private final char quoteChar;

    private final String nullString;

    private int numExpectedFields;

    private final int lenientPolicy;

    private final List<String> tokens;

    /**
     * Creates a new instance.
     *
     * @param fieldSeparator is the character that separates fields
     * @param quoteChar      is the character that is used to quote fields (although unquoted fields are allowed as well)
     * @param nullString     is the representation of null values or {@code null} if none
     */
    public CsvParser(final char fieldSeparator, final char quoteChar, final String nullString) {
        this(fieldSeparator, quoteChar, nullString, -1, -1);
    }

    /**
     * Creates a new instance.
     *
     * @param fieldSeparator    is the character that separates fields
     * @param quoteChar         is the character that is ued to quote fields (although unquoted fields are allowed as well)
     * @param numExpectedFields is the number of fields expected in each row
     * @param lenientPolicy     defines how to react if the number of expected fields is not met by a row (see
     *                          {@link #FAIL_ON_ILLEGAL_LINES}, {@link #FAIL_ON_OVERLONG_LINES}, {@link #WARN_ON_ILLEGAL_LINES})
     */
    public CsvParser(final char fieldSeparator, final char quoteChar, final String nullString, final int numExpectedFields,
                     final int lenientPolicy) {

        this.fieldSeparator = fieldSeparator;
        this.quoteChar = quoteChar;
        this.nullString = nullString;
        this.numExpectedFields = numExpectedFields;
        this.lenientPolicy = lenientPolicy;
        this.tokens = new ArrayList<>();
    }

    /**
     * Parses the given row into fields.
     *
     * @param row is the row to parse
     * @return a list containing the fields of the row; empty fields will be set to {@code null} and the list might be
     * reused on the next {@link #parse(String)} call.
     * @throws Exception it the given row is not well-formatted
     */
    public List<String> parse(final String row) throws Exception {
        this.tokens.clear();

        int scanOffset = 0;
        while (scanOffset <= row.length()) {
            scanOffset = readNextField(row, scanOffset);

            // Check if we have a null value.
            if (this.nullString != null) {
                final int lastIndex = this.tokens.size() - 1;
                String lastToken = this.tokens.get(lastIndex);
                if (this.nullString.equals(lastToken)) {
                    this.tokens.set(lastIndex, null);
                }
            }
        }

        // Make sanity check if we found the expected number of fields.
        if (this.numExpectedFields >= 0) {
            final int numEncounteredFields = this.tokens.size();

            if (numEncounteredFields != this.numExpectedFields) {
                final String msg = String.format("Found %d field, expected %d: <<<%s>>>", numEncounteredFields,
                        this.numExpectedFields, row);
                if (this.lenientPolicy == FAIL_ON_ILLEGAL_LINES
                        || (this.lenientPolicy == FAIL_ON_OVERLONG_LINES && numEncounteredFields > this.numExpectedFields)) {
                    throw new RuntimeException(msg);
                } else {
                    logger.warn(msg);
                }
            }
        }

        return this.tokens;
    }

    /**
     * Writes the next field of the line (starting from scanOffset) into {@link #tokens}.
     *
     * @param row        is the row to parse
     * @param scanOffset is an offset within row from which the parsing should start
     * @return the position after the field's trailing field separator or EOL
     */
    private int readNextField(final String row, int scanOffset) {

        if (row.length() == scanOffset) {
            this.tokens.add("");
            return scanOffset + 1;
        }

        int state = 0; // 0: beginning, 1: normal mode, 2: in quote, 3: after quote
        int fieldStartPos = -1;
        int fieldEndPos = -1;

        boolean isFoundDoubleQuotes = false;
        boolean isEol = true;

        ScanLoop:
        while (scanOffset < row.length()) {
            final char charAtScanOffset = row.charAt(scanOffset);
            switch (state) {
                case 0: // beginning
                    if (charAtScanOffset == this.quoteChar) {
                        fieldStartPos = scanOffset + 1;
                        state = 2;

                    } else if (charAtScanOffset == this.fieldSeparator) {
                        fieldStartPos = scanOffset;
                        fieldEndPos = scanOffset;
                        isEol = false;
                        break ScanLoop;

                    } else if (charAtScanOffset != ' ' && charAtScanOffset != '\t') {
                        fieldStartPos = scanOffset;
                        state = 1;
                    }
                    break;

                case 1: // normal mode
                    if (charAtScanOffset == this.fieldSeparator) {
                        fieldEndPos = scanOffset;
                        isEol = false;
                        break ScanLoop;
                    }
                    break;

                case 2: // in quote
                    if (charAtScanOffset == this.quoteChar) {
                        state = 3;
                        fieldEndPos = scanOffset;
                    }
                    break;

                case 3: // after quote
                    if (charAtScanOffset == this.quoteChar && scanOffset == fieldEndPos + 1) {
                        // allow to return to in-quote state
                        state = 2;
                        isFoundDoubleQuotes = true;
                    } else if (charAtScanOffset == this.fieldSeparator) {
                        isEol = false;
                        break ScanLoop;
                    } else if (charAtScanOffset != ' ' && charAtScanOffset != '\t' && charAtScanOffset != '\r') {
                        throw new IllegalArgumentException(String.format(
                                "Expected '%s' or white space after quote at %d (found '%s') in line [%s].", this.fieldSeparator,
                                scanOffset, Character.valueOf(charAtScanOffset), row));
                    }
            }
            scanOffset++;
        }

        if (fieldStartPos == -1) {
            fieldStartPos = scanOffset;
        }

        if (fieldEndPos == -1) {
            fieldEndPos = scanOffset;
        }

        // scan offset either points to a field separator or EOL
        final int valueLength = fieldEndPos - fieldStartPos;
        if (valueLength > 0) {
            String field = row.substring(fieldStartPos, fieldEndPos);
            if (isFoundDoubleQuotes) {
                field = field.replaceAll("\"\"", "\"");
            }
            this.tokens.add(field);
        } else {
            this.tokens.add("");
        }

        scanOffset++;
        if (isEol) {
            scanOffset++;
        }
        return scanOffset;
    }

    /**
     * @param numExpectedFields the numExpectedFields to set
     */
    public void setNumExpectedFields(int numExpectedFields) {
        this.numExpectedFields = numExpectedFields;
    }

}
