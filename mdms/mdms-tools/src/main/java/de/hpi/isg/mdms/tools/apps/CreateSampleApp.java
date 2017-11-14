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

import au.com.bytecode.opencsv.CSVParser;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.location.CsvFileLocation;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.TableSample;
import de.hpi.isg.mdms.flink.util.FileUtils;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.apache.flink.core.fs.Path;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * This job creates {@link TableSample} using reservoir sampling.
 *
 * @author Sebastian Kruse
 */
public class CreateSampleApp extends MdmsAppTemplate<CreateSampleApp.Parameters> {

    private static final String CONSTRAINT_COLLECTION_ID_RESULT_KEY = "constraintCollectionId";

    /**
     * Creates a new instance.
     *
     * @see CsvAppTemplate#CsvAppTemplate(Object)
     */
    public CreateSampleApp(final CreateSampleApp.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Detect the schema.
        Schema schema = this.metadataStore.getSchemaByName(this.parameters.schemaName);

        // Do the profiling.
        Collection<TableSample> samples = profileSamples(
                this.metadataStore,
                schema,
                this.parameters.sampleSize,
                this.parameters.seed
        );

        // Store the results.
        String ccDescription = String.format(
                "Table samples (size=%,d, seed=%d)",
                this.parameters.sampleSize,
                this.parameters.seed
        );
        ConstraintCollection<TableSample> constraintCollection = this.metadataStore.createConstraintCollection(
                ccDescription, TableSample.class, schema
        );
        samples.forEach(constraintCollection::add);
        this.metadataStore.flush();
        this.logger.info(String.format("Saved constraint collection %d.", constraintCollection.getId()));

        // Add some metadata about the program results.
        this.executionMetadata.addCustomData(CONSTRAINT_COLLECTION_ID_RESULT_KEY, constraintCollection.getId());
    }

    /**
     * Profile all tables of a {@link Schema} for tuple samples.
     *
     * @param store      within which the {@code schema} resides
     * @param schema     the {@link Schema} whose (CSV) tables should be profiled
     * @param seed       to make the sampling repeatable
     * @param sampleSize the (maximum) size of the sample
     * @return the {@link TableSample}s
     */
    public static Collection<TableSample> profileSamples(
            MetadataStore store,
            Schema schema,
            int sampleSize,
            int seed) {

        Collection<TableSample> samples = new ArrayList<>();

        // Go over the files and create the samples.
        for (Table table : schema.getTables()) {
            // Get the CSV file location.
            Location location = table.getLocation();
            if (!(location instanceof CsvFileLocation)) {
                LoggerFactory.getLogger(CreateSampleApp.class).error(
                        "Cannot process {} at {}. Only CSV files are supported. Skipping...", table, location
                );
                continue;
            }
            CsvFileLocation csvFileLocation = (CsvFileLocation) location;

            // Prepare the sampling.
            Random random = new Random(seed);
            int rowNum = 0;
            String[][] sample = new String[sampleSize][];
            CSVParser csvParser = new CSVParser(
                    csvFileLocation.getFieldSeparator(),
                    csvFileLocation.getQuoteChar(),
                    '\0',
                    false,
                    true
            );
            try (BufferedReader bufferedReader = new BufferedReader(
                    csvFileLocation.getEncoding().applyTo(
                            FileUtils.open(new Path(csvFileLocation.getPath()), null)
                    ))) {

                // Go over the file and sample tuples via reservoir sampling.
                if (csvFileLocation.getHasHeader()) bufferedReader.readLine();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    // Find out, whether to include the line in the sample (and where).
                    int index = rowNum < sampleSize ? rowNum : random.nextInt(sampleSize);
                    if (index < sampleSize) {
                        String[] fields = csvParser.parseLine(line);
                        sample[index] = fields;
                    }
                    rowNum++;
                }

                // Create the sample.
                if (rowNum < sampleSize) {
                    sample = Arrays.copyOf(sample, rowNum);
                }
                samples.add(new TableSample(table.getId(), sample));

            } catch (Exception e) {
                LoggerFactory.getLogger(CreateSampleApp.class).error("Processing {} failed.", table, e);
            }
        }
        return samples;
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    public static void main(final String[] args) throws Exception {
        CreateSampleApp.Parameters parameters = new CreateSampleApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new CreateSampleApp(parameters).run();
    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    @SuppressWarnings("serial")
    public static class Parameters implements Serializable {

        @Parameter(names = {MetadataStoreParameters.SCHEMA_NAME}, description = "the name of the schema", required = true)
        public String schemaName;

        @Parameter(names = "--seed", description = "seed to initialize the random projections")
        private int seed = 42;

        @Parameter(names = "--sample-size", description = "the (maximum) size of the sample")
        private int sampleSize = 100;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

    }

}
