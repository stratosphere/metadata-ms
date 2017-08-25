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
import de.hpi.isg.mdms.flink.location.AbstractCsvLocation;
import de.hpi.isg.mdms.flink.location.CsvFileLocation;
import de.hpi.isg.mdms.flink.parameters.FlinkParameters;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.tools.sqlParser.TableCreationStatementParser;
import de.hpi.isg.mdms.tools.util.CsvUtils;
import org.apache.flink.core.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * This job creates a {@link Schema} for the given files and saves it to a {@link MetadataStore}.
 *
 * @author Sebastian Kruse
 */
public class CreateSchemaForCsvFilesApp extends CsvAppTemplate<CreateSchemaForCsvFilesApp.Parameters> {

    private static final String SCHEMA_ID_RESULT_KEY = "schemaId";

    /**
     * Creates a new instance.
     *
     * @see CsvAppTemplate#CsvAppTemplate(Object)
     */
    public CreateSchemaForCsvFilesApp(final CreateSchemaForCsvFilesApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds, String fileLocation, String schemaName,
                                      String fieldSeparator, String quoteChar, String hasHeader) throws Exception {

        CreateSchemaForCsvFilesApp.Parameters parameters = new CreateSchemaForCsvFilesApp.Parameters();

        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(fileLocation);
        parameters.inputFiles = inputFiles;

        parameters.schemaName = schemaName;
        parameters.hasHeader = hasHeader;
        parameters.csvParameters.fieldSeparatorName = fieldSeparator;
        parameters.csvParameters.quoteCharName = quoteChar;

        CreateSchemaForCsvFilesApp app = new CreateSchemaForCsvFilesApp(parameters);
        app.metadataStore = mds;

        app.run();
    }

    @Override
    protected void executeProgramLogic(final List<Path> files) throws Exception {
        HashMap<String, List<String>> columnNamesMap = null;
        if (!this.parameters.sqlFile.isEmpty()) {
            TableCreationStatementParser sqlParser = new TableCreationStatementParser();
            columnNamesMap = sqlParser.getColumnNameMap(this.parameters.sqlFile);
        }

        final NameProvider nameProvider = this.parameters.createNameProvider();

        final String schemaName = this.parameters.schemaName;
        for (final Schema existingSchema : this.metadataStore.getSchemas()) {
            if (existingSchema.getName().equals(schemaName)) {
                getLogger().warn("The existing schema {} already has the name \"{}\". Press enter to continue...",
                        existingSchema.getId(), schemaName);
                new BufferedReader(new InputStreamReader(System.in)).readLine();
            }
        }
        final Location schemaLocation = this.parameters.inputFiles.size() == 1 ?
                DefaultLocation.createForFile(this.parameters.inputFiles.get(0)) : null;
        final Schema schema = this.metadataStore.addSchema(schemaName, "", schemaLocation);
        logger.info("added schema {} with ID {}", schema.getName(), schema.getId());
        for (final Path file : files) {
            final String tableName = nameProvider.provideTableName(file);
            final AbstractCsvLocation tableLocation = new CsvFileLocation();
            String path = file.toString();
            tableLocation.setPath(path);
            tableLocation.setFieldSeparator(this.parameters.csvParameters.getFieldSeparatorChar());
            tableLocation.setQuoteChar(this.parameters.csvParameters.getQuoteChar());
            tableLocation.setEncoding(this.fileEncodings.get(path));
            tableLocation.setHasHeader((this.parameters.hasHeader).equalsIgnoreCase("true"));
            tableLocation.setNullString(this.parameters.csvParameters.getNullString());
            final Table table = schema.addTable(this.metadataStore, tableName, "", tableLocation);
            logger.info("added table {} with ID {}", table.getName(), table.getId());
            String[] columnNames = new String[0];
            if (tableLocation.getHasHeader()) {
                columnNames = CsvUtils.getColumnNames(file, this.parameters.csvParameters.getFieldSeparatorChar(),
                        this.parameters.csvParameters.getQuoteChar(), null);
            }
            final int numAttributes = this.attributeIndexer.getNumAttributes(tableName);
            for (int attributeIndex = 0; attributeIndex < numAttributes; attributeIndex++) {
                final String attributeName;
                if (tableLocation.getHasHeader()) {
                    attributeName = columnNames[attributeIndex];
                }
                else if (columnNamesMap!=null && columnNamesMap.get(tableName)!=null && columnNamesMap.get(tableName).size()>attributeIndex) {
                        attributeName = columnNamesMap.get(tableName).get(attributeIndex);
                }
                else {
                    attributeName = nameProvider.provideColumnName(attributeIndex);
                }
                final Column column = table.addColumn(this.metadataStore, attributeName, "", attributeIndex);
                logger.info("added column {} with ID {}", column.getName(), column.getId());

            }
        }

        this.metadataStore.flush();
        logger.info(String.format("Saved schema (%d, %s):\n%s.\n", schema.getId(), schemaName, schema));

        // Add some metadata about the program results.
        this.executionMetadata.addCustomData(SCHEMA_ID_RESULT_KEY, schema.getId());
    }

    @Override
    protected Collection<String> getInputFiles() {
        return this.parameters.inputFiles;
    }

    @Override
    protected FlinkParameters getFlinkParameters() {
        return this.parameters.stratosphereParameters;
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
        return this.parameters.dirDepth;
    }

    public static void main(final String[] args) throws Exception {
        CreateSchemaForCsvFilesApp.Parameters parameters = new CreateSchemaForCsvFilesApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new CreateSchemaForCsvFilesApp(parameters).run();
    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    @SuppressWarnings("serial")
    public static class Parameters implements Serializable {

        @Parameter(required = true)
        public List<String> inputFiles;

        @Parameter(names = {MetadataStoreParameters.SCHEMA_NAME}, description = "the name of the schema", required = true)
        public String schemaName;

        @Parameter(names = {"--dir-depth"}, description = "the recursion depth for gathering files from directories")
        public int dirDepth = 1;

        @Parameter(names = {"--has-header"}, description = "whether the first line of the file shall be used to identify the column names (true, false)")
        public String hasHeader = "false";

        @Parameter(names = {"--sql-file"}, description = "an additional sql file containing CREATE TABLE statements to help identifying the column names")
        public String sqlFile = "";

        @Parameter(names = {"--name-provider"},
                description = "how to generate names for the schema elements (sodap/metanome)",
                required = false)
        public String nameProvider = "sodap";

        /**
         * Creates a new {@link NameProvider} based on the {@link #nameProvider} attribute.
         *
         * @return the {@link NameProvider}
         */
        public NameProvider createNameProvider() {
            if (this.nameProvider.equalsIgnoreCase("metanome")) {
                return new MetanomeNameProvider();
            }
            return new SodapNameProvider();
        }

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @ParametersDelegate
        public final FlinkParameters stratosphereParameters = new FlinkParameters();

        @ParametersDelegate
        public final CsvParameters csvParameters = new CsvParameters();

    }

    /**
     * Provide default names for schema elements.
     */
    private interface NameProvider {

        String provideTableName(Path file);

        String provideColumnName(int offset);

    }

    /**
     * Names MDMS-style.
     */
    private static class SodapNameProvider implements NameProvider {

        @Override
        public String provideTableName(Path file) {
            return file.getName();
        }

        @Override
        public String provideColumnName(int offset) {
            return String.format("[%d]", offset);
        }
    }

    /**
     * Names Metanome-style.
     */
    private static class MetanomeNameProvider implements NameProvider {

        @Override
        public String provideTableName(Path file) {
            return file.getName();
        }

        @Override
        public String provideColumnName(int offset) {
            return String.format("column%d", offset + 1);
        }
    }

}
