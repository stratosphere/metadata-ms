/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.tools.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.parameters.CsvParameters;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.flink.functions.ParsePlainCsvRows;
import de.hpi.isg.mdms.flink.location.MergedCsvFileLocation;
import de.hpi.isg.mdms.flink.location.MergedCsvFilePartitionLocation;
import de.hpi.isg.mdms.flink.parameters.FlinkParameters;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * This job creates a {@link Schema} for the given files and saves it to a {@link MetadataStore}.
 * {@code MCSV} files are <i>merged CSV files</i>, in which the first field in each row represents the table, to which
 * this row belongs. The remaining fields are actual fields. In consequence, MCSV files are jagged, i.e., their rows
 * differ in the number of fields.
 *
 * @author Sebastian Kruse
 */
public class CreateSchemaForMcsvFileApp extends CsvAppTemplate<CreateSchemaForMcsvFileApp.Parameters> {

    /**
     * Counts the number of added tables in this program.
     */
    private int numTables = 0;

    /**
     * Counts the number of encountered empty tables in this program.
     */
    private int numEmptyTables = 0;

    /**
     * Counts the number of added columns.
     */
    private int numColumns = 0;

    /**
     * The schema that is created by this program.
     */
    private Schema schema;


    /**
     * Creates a new instance.
     *
     * @see CsvAppTemplate#CsvAppTemplate(Object)
     */
    public CreateSchemaForMcsvFileApp(final CreateSchemaForMcsvFileApp.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void executeProgramLogic(final List<Path> files) throws Exception {

        if (files.isEmpty()) {
            throw new IllegalArgumentException("No input files detected.");
        }
        // Use the originally passed file as input, it should work with #readTextFile() even if it is a directory/
        final String inputFilePath = this.parameters.inputFile;
        // final Path inputFilePath = files.get(0);

        final String schemaName = this.parameters.schemaName;
        for (final Schema existingSchema : this.metadataStore.getSchemas()) {
            if (existingSchema.getName().equals(schemaName)) {
                getLogger().warn("The existing schema {} already has the name \"{}\". Press enter to continue...",
                        existingSchema.getId(), schemaName);
                new BufferedReader(new InputStreamReader(System.in)).readLine();
            }
        }

        // Run a job to determine the maximum number of fields for each table.
        DataSet<Tuple2<Integer, Integer>> numAttributesDataSet = this.executionEnvironment
                .readTextFile(inputFilePath)
                // FIXME: null because we need actual input files
                .map(new ParsePlainCsvRows(getCsvParameters().getFieldSeparatorChar(), getCsvParameters()
                        .getQuoteChar(), this.getCsvParameters().getNullString()))
                .map(new CountFields())
                .groupBy(0)
                .max(1).andSum(2)
                .filter(new FilterSmallTables(this.parameters.minTuples))
                .project(0, 1);

        // if available, load the table names and join them to the field counts
        if (this.parameters.idInputFile != null) {
            DataSet<Tuple2<Integer, String>> tableNames = this.executionEnvironment
                    .readTextFile(this.parameters.idInputFile)
                    .map(new ParsePlainCsvRows(getCsvParameters().getFieldSeparatorChar(), getCsvParameters().getQuoteChar(), this.getCsvParameters().getNullString()))
                    .map(new ExtractTableIds());

            DataSet<Tuple3<Integer, Integer, String>> result = numAttributesDataSet
                    .join(tableNames)
                    .where(0).equalTo(0)
                    .with(new JoinTableNames());

            RemoteCollectorImpl.collectLocal(result,
                    tableDescriptor -> {
                        addTable(tableDescriptor.f0, tableDescriptor.f1, tableDescriptor.f2);
                    });

        } else {
            RemoteCollectorImpl.collectLocal(numAttributesDataSet,
                    fieldCount -> {
                        addTable(fieldCount.f0, fieldCount.f1, null);
                    });
        }

        // Configure the schema element.
        final MergedCsvFileLocation schemaLocation = new MergedCsvFileLocation();
        schemaLocation.setPath(inputFilePath);
        schemaLocation.setFieldSeparator(getCsvParameters().getFieldSeparatorChar());
        schemaLocation.setQuoteChar(getCsvParameters().getQuoteChar());
        schemaLocation.setNullString(this.parameters.csvParameters.getNullString());
        String description = String.format("created from %s (%s)", this.parameters.inputFile, DateFormat.getInstance().format(new Date()));
        this.schema = this.metadataStore.addSchema(schemaName, description, schemaLocation);
        logger.debug("added schema {} with ID {}", schema.getName(), schema.getId());

        // XXX: Little hack: clear the schema cache, so that the new created tables will not be kept in memory
        if (schema instanceof RDBMSSchema) {
            ((RDBMSSchema) schema).cacheChildTables(null);
        }

        // Run the Flink job.
        executePlan(String.format("Determine #fields in %s.", inputFilePath));

        // Save the changes.
        this.metadataStore.flush();

        // Print an informative summary.
        logger.info("Saved schema {} (ID={})", schemaName, this.schema.getId());
        logger.info("{} tables, {} columns", numTables, numColumns);
        if (this.numEmptyTables > 0) {
            if (this.parameters.isDropEmptyTables) {
                logger.info("Did not include {} empty tables.", numEmptyTables);
            } else {
                logger.info("{} tables are empty.", numEmptyTables);
            }
        }
    }

    private void addTable(int inFileId, int numAttributes, String tableName) {
        synchronized (this.schema) {
            // Count and optionally drop tables without attributes.
            if (numAttributes == 0) {
                this.numEmptyTables++;
                if (this.parameters.isDropEmptyTables) {
                    return;
                }
            }

            if (tableName == null) {
                tableName = String.format("T%d", inFileId);
            }
            final MergedCsvFilePartitionLocation tableLocation = new MergedCsvFilePartitionLocation();
            tableLocation.setPartitionId(inFileId);
            final Table table = this.schema.addTable(this.metadataStore, tableName, "", tableLocation);
            logger.debug("added table {} with ID {}", table.getName(), table.getId());
            this.numTables++;

            // Configure columns.
            for (int attributeIndex = 0; attributeIndex < numAttributes; attributeIndex++) {
                final String attributeName = String.format("[%d]", attributeIndex);
                final Column column = table.addColumn(this.metadataStore, attributeName, "", attributeIndex);
                logger.debug("added column {} with ID {}", column.getName(), column.getId());
            }
            this.numColumns += numAttributes;

            if (this.numTables % 10000 == 0) {
                logger.info("Added {} tables so far...", this.numTables);
            }
        }
    }

    @Override
    protected Collection<String> getInputFiles() {
        return Collections.singletonList(this.parameters.inputFile);
    }

    @Override
    protected FlinkParameters getFlinkParameters() {
        return this.parameters.flinkParameters;
    }

    @Override
    protected CsvParameters getCsvParameters() {
        return this.parameters.csvParameters;
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    @Override
    protected int getFileFetchDepth() {
        // Allow that the actual input file is a folder w/ partial files.
        return 1;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected void onExit() {
        super.onExit();
        System.exit(0);
    }

    public static void main(final String[] args) throws Exception {
        CreateSchemaForMcsvFileApp.Parameters parameters = new CreateSchemaForMcsvFileApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new CreateSchemaForMcsvFileApp(parameters).run();
    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    @SuppressWarnings("serial")
    public static class Parameters implements Serializable {

        @Parameter(names = "--input", required = true)
        public String inputFile;

        @Parameter(names = "--id-input", required = false)
        public String idInputFile;

        @Parameter(names = {MetadataStoreParameters.SCHEMA_NAME}, description = "the name of the schema", required = true)
        public String schemaName;

        @Parameter(names = {"--drop-empty-tables"}, description = "whether empty tables shall not be included in the schema")
        public boolean isDropEmptyTables = false;

        @Parameter(names = {"--min-tuples"}, description = "tables with less tuples will be rejected")
        public int minTuples = 0;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @ParametersDelegate
        public final FlinkParameters flinkParameters = new FlinkParameters();

        @ParametersDelegate
        public final CsvParameters csvParameters = new CsvParameters();

    }

    @SuppressWarnings("serial")
    public static class CountFields implements MapFunction<String[], Tuple3<Integer, Integer, Integer>> {

        private final Tuple3<Integer, Integer, Integer> outputTuple = new Tuple3<>(null, null, 1);

        @Override
        public Tuple3<Integer, Integer, Integer> map(String[] fieldsWithId) throws Exception {
            this.outputTuple.f0 = Integer.parseInt(fieldsWithId[0]);
            this.outputTuple.f1 = fieldsWithId.length - 1;
            return this.outputTuple;
        }

    }

    @SuppressWarnings("serial")
    public static class ExtractTableIds implements MapFunction<String[], Tuple2<Integer, String>> {

        private final Tuple2<Integer, String> outputTuple = new Tuple2<>();

        @Override
        public Tuple2<Integer, String> map(String[] fieldsWithId) throws Exception {
            this.outputTuple.f0 = Integer.parseInt(fieldsWithId[0]);
            this.outputTuple.f1 = fieldsWithId[1];
            return this.outputTuple;
        }

    }


    @SuppressWarnings("serial")
    public static class JoinTableNames implements JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, String>, Tuple3<Integer, Integer, String>> {

        private final Tuple3<Integer, Integer, String> outputTuple = new Tuple3<>();

        @Override
        public Tuple3<Integer, Integer, String> join(Tuple2<Integer, Integer> fieldCount, Tuple2<Integer, String> attributeName)
                throws Exception {

            this.outputTuple.f0 = fieldCount.f0;
            this.outputTuple.f1 = fieldCount.f1;
            this.outputTuple.f2 = attributeName.f1;

            return this.outputTuple;
        }
    }

    public static class FilterSmallTables implements FilterFunction<Tuple3<Integer, Integer, Integer>> {

        private final int threshold;

        public FilterSmallTables(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
            return value.f2 >= this.threshold;
        }
    }

}
