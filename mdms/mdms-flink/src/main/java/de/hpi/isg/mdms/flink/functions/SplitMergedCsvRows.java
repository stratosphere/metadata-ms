package de.hpi.isg.mdms.flink.functions;

import de.hpi.isg.mdms.flink.util.CsvParser;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * This function splits a given merged CSV row into its fields. The given first integer field will be discarded and the
 * output field IDs are solely computed from the CSV rows.
 * 
 * @author Sebastian Kruse
 */
public class SplitMergedCsvRows extends RichFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

	private static final long serialVersionUID = 1377116120504051734L;

	private static final Logger LOG = LoggerFactory.getLogger(SplitMergedCsvRows.class);

	private final Tuple2<Integer, String> outputTuple = new Tuple2<Integer, String>();

	private final boolean isSupressingEmptyFields;

	private final CsvParser csvRowParser;

	private int lenientPolicy;

	private final Int2IntMap idDictionary;

	/**
	 * Creates a new instance without checks of the number of fields in the rows and limitation of used fields but with
	 * supressing of empty fields.
	 * 
	 * @param fieldSeparator
	 *            is the character that separates fields
	 * @param quoteChar
	 *            is the character that is used to quote fields (although unquoted fields are allowed as well)
	 */
	public SplitMergedCsvRows(final char fieldSeparator, final char quoteChar) {
		this(fieldSeparator, quoteChar, -1, true);
	}

	/**
	 * Creates a new instance without limitation of used fields.
	 * 
	 * @param fieldSeparator
	 *            is the character that separates fields
	 * @param quoteChar
	 *            is the character that is used to quote fields (although unquoted fields are allowed as well)
	 * @param lenientPolicy
	 *            describes the behavior of the parser on illegal number of fields in a row
	 * @param isSupressingEmptyFields
	 *            tells whether empty fields will be forwarded by this operator or supressed
	 */
	public SplitMergedCsvRows(final char fieldSeparator, final char quoteChar, final int lenientPolicy,
							  boolean isSupressingEmptyFields) {

		this(fieldSeparator, quoteChar, lenientPolicy, isSupressingEmptyFields, null, null);
	}

	/**
	 * Creates a new instance without limitation of used fields.
	 * 
	 * @param fieldSeparator
	 *            is the character that separates fields
	 * @param quoteChar
	 *            is the character that is used to quote fields (although unquoted fields are allowed as well)
	 * @param lenientPolicy
	 *            describes the behavior of the parser on illegal number of fields in a row
	 * @param isSupressingEmptyFields
	 *            tells whether empty fields will be forwarded by this operator or supressed
	 * @param idDictionary
	 *            is a translation of table IDs within the merged CSV file to metadata store IDs
	 * @param idUtils are the {@link IdUtils} of the metadata store (only necessary when an ID dictionary is used)
	 * 
	 */
	public SplitMergedCsvRows(final char fieldSeparator, final char quoteChar, final int lenientPolicy,
							  boolean isSupressingEmptyFields, Int2IntMap idDictionary, IdUtils idUtils) {

		this.csvRowParser = new CsvParser(fieldSeparator, quoteChar, -1, lenientPolicy);
		this.isSupressingEmptyFields = isSupressingEmptyFields;
		this.lenientPolicy = lenientPolicy;

		// Build ID map if necessary.
		if (idDictionary == null) {
			this.idDictionary = null;
		} else {
			this.idDictionary = new Int2IntOpenHashMap(idDictionary.size());
			this.idDictionary.defaultReturnValue(-1);
			for (Int2IntOpenHashMap.Entry originalIdDictEntry : idDictionary.int2IntEntrySet()) {
				int fileId = originalIdDictEntry.getIntKey();
				int tableId = originalIdDictEntry.getIntValue();
				int minColumnId = idUtils.createGlobalId(
						idUtils.getLocalSchemaId(tableId),
						idUtils.getLocalTableId(tableId),
						idUtils.getMinColumnNumber());
				this.idDictionary.put(fileId, minColumnId);
			}
		}
	}

	@Override
	public void flatMap(final Tuple2<Integer, String> fileLine, final Collector<Tuple2<Integer, String>> out)
			throws Exception {

		final String row = fileLine.f1;

		// Configure and run the parser.
		List<String> fields;
		try {
			fields = this.csvRowParser.parse(row);
		} catch (Exception e) {
			switch (this.lenientPolicy) {
			case CsvParser.WARN_ON_ILLEGAL_LINES:
				LOG.warn("Could not parse a line correctly.", e);
				return;
			default:
				LOG.warn("Unknown lenient policy {}. Fallback to fail policy.", String.valueOf(this.lenientPolicy));
			case CsvParser.FAIL_ON_ILLEGAL_LINES:
				throw e;
			}
		}

		// Forward the parsed values.
		Iterator<String> i = fields.iterator();
		// TODO: This is unsafe if a table has more than 2^6 = 64 columns. Outsource the ID setting to the CSV merge
		// process. Attention with the sampling then.
		int tableId = Integer.parseInt(i.next());
		int minAttributeId;
		if (this.idDictionary != null) {
			minAttributeId = this.idDictionary.get(tableId);
			if (minAttributeId == -1) {
				return;
			}
		} else {
			minAttributeId = tableId << 8;
		}
		
		minAttributeId--;
		while (i.hasNext()) {
		    minAttributeId++;

		    String field = i.next();
			if (field == null) {
				if (this.isSupressingEmptyFields) {
					continue;
				} else {
					field = "";
				}
			}
			this.outputTuple.f0 = minAttributeId;
			this.outputTuple.f1 = field;
			out.collect(this.outputTuple);
		}
	}

}
