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
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.location.CsvFileLocation;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.Signature;
import de.hpi.isg.mdms.domain.constraints.Vector;
import de.hpi.isg.mdms.flink.util.FileUtils;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.apache.flink.core.fs.Path;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Serializable;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

/**
 * This job creates {@link Vector}s of Q-gram sketches for columns and saves them to a {@link MetadataStore}.
 * <p>See: Dasu, Tamraparni, et al. "Mining database structure; or, how to build a data quality browser."
 * Proceedings of the 2002 ACM SIGMOD international conference on Management of data. ACM, 2002.</p>
 *
 * @author Sebastian Kruse
 */
public class CreateQGramSketchApp extends MdmsAppTemplate<CreateQGramSketchApp.Parameters> {

    private static final String CONSTRAINT_COLLECTION_ID_RESULT_KEY = "constraintCollectionId";

    /**
     * Creates a new instance.
     *
     * @see CsvAppTemplate#CsvAppTemplate(Object)
     */
    public CreateQGramSketchApp(final CreateQGramSketchApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds, String schemaName,
                                      int q, int numSketchDimensions, int numQGramDimensions) throws Exception {

        CreateQGramSketchApp.Parameters parameters = new CreateQGramSketchApp.Parameters();

        parameters.schemaName = schemaName;
        parameters.q = q;
        parameters.numSketchDimensions = numSketchDimensions;
        parameters.numQGramDimensions = numQGramDimensions;
        parameters.metadataStoreParameters.isCloseMetadataStore = false;

        CreateQGramSketchApp app = new CreateQGramSketchApp(parameters);
        app.metadataStore = mds;

        app.run();
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Detect the schema.
        Schema schema = this.metadataStore.getSchemaByName(this.parameters.schemaName);

        // Do the profiling.
        Collection<Vector> vectors = profileQGramSketches(
                this.metadataStore,
                schema,
                this.parameters.numSketchDimensions,
                this.parameters.numQGramDimensions,
                this.parameters.seed,
                this.parameters.q
        );

        // Store the results.
        String ccDescription = String.format(
                "Q-gram sketches (q=%d, dim=%d, seed=%d)",
                this.parameters.q,
                this.parameters.numSketchDimensions,
                this.parameters.seed
        );
        ConstraintCollection<Vector> constraintCollection = this.metadataStore.createConstraintCollection(
                ccDescription, Vector.class, schema
        );
        vectors.forEach(constraintCollection::add);
        this.metadataStore.flush();
        this.logger.info(String.format("Saved constraint collection %d.", constraintCollection.getId()));

        // Add some metadata about the program results.
        this.executionMetadata.addCustomData(CONSTRAINT_COLLECTION_ID_RESULT_KEY, constraintCollection.getId());
    }

    /**
     * Profile all tables of a {@link Schema} for q-gram sketches (dimension-reduced q-gram vectors).
     *
     * @param store               within which the {@code schema} resides
     * @param schema              the {@link Schema} whose (CSV) tables should be profiled
     * @param numSketchDimensions the number of dimensions to sketch the space of q-grams
     * @param numQGramDimensions  the dimensionality of the resulting q-gram {@link Vector}s
     * @param seed                to create random transformation matrix from the sketch space to the q-gram vector space
     * @param q                   the size of the q-grams
     * @return the q-gram {@link Vector}s
     */
    public static Collection<Vector> profileQGramSketches(
            MetadataStore store,
            Schema schema,
            int numSketchDimensions,
            int numQGramDimensions,
            int seed,
            int q) {

        List<Vector> qGramVectors = new ArrayList<>();

        // Initialize the random projections.
        Random random = new Random(seed);
        List<ToDoubleFunction<int[]>> projections = new ArrayList<>(numSketchDimensions);
        for (int sketchDimension = 0; sketchDimension < numSketchDimensions; sketchDimension++) {
            final int[] projectionVector = new int[numQGramDimensions];
            for (int qGramDimension = 0; qGramDimension < numQGramDimensions; qGramDimension++) {
                projectionVector[qGramDimension] = random.nextInt(2) * 2 - 1;
            }
            projections.add(
                    sketchDimension,
                    qGramVector -> {
                        assert projectionVector.length == qGramVector.length;
                        double dotProduct = 0d;
                        long qGramVectorSum = 0;
                        for (int i = 0; i < projectionVector.length; i++) {
                            dotProduct += projectionVector[i] * qGramVector[i];
                            qGramVectorSum += qGramVector[i];
                        }
                        return dotProduct / qGramVectorSum;
                    }
            );
        }

        // Initialize a hash function to map the q-grams to positions in the
        HashFunction hashFunction = Hashing.murmur3_32();
        byte[] qGram = new byte[q * 2];

        // Go over the files and create the sketches.
        for (Table table : schema.getTables()) {
            // Get the CSV file location.
            Location location = table.getLocation();
            if (!(location instanceof CsvFileLocation)) {
                LoggerFactory.getLogger(CreateQGramSketchApp.class).error(
                        "Cannot process {} at {}. Only CSV files are supported. Skipping...", table, location
                );
                continue;
            }
            CsvFileLocation csvFileLocation = (CsvFileLocation) location;

            // Prepare the q-gram vector calculation.
            int[][] qGramMatrix = new int[table.getColumns().size()][numQGramDimensions];
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

                // Go over the file and collect the q-grams.
                if (csvFileLocation.getHasHeader()) bufferedReader.readLine();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] fields = csvParser.parseLine(line);
                    for (int fieldIndex = 0; fieldIndex < Math.max(fields.length, qGramMatrix.length); fieldIndex++) {
                        String field = fields[fieldIndex];
                        if (field == null || field.isEmpty()) continue;

                        // Create the q-grams.
                        for (int start = 1 - q; start < field.length(); start++) {
                            // Assemble the q-gram.
                            for (int offset = 0; offset < q; offset++) {
                                int pos = start + offset;
                                char c = pos < 0 || pos >= field.length() ? '\0' : field.charAt(pos);
                                qGram[2 * offset] = (byte) (c >>> 8);
                                qGram[2 * offset + 1] = (byte) c;
                            }

                            // Put it into the q-gram vector.
                            int qQgramPosition = Math.abs(hashFunction.hashBytes(qGram).asInt()) % numQGramDimensions;
                            qGramMatrix[fieldIndex][qQgramPosition]++;
                        }
                    }
                }

                // Create the sketches.
                for (int columnIndex = 0; columnIndex < qGramMatrix.length; columnIndex++) {
                    int[] qGramVector = qGramMatrix[columnIndex];
                    double[] sketch = new double[numSketchDimensions];
                    int sketchDimension = 0;
                    for (ToDoubleFunction<int[]> projection : projections) {
                        sketch[sketchDimension++] = projection.applyAsDouble(qGramVector);
                    }

                    int schemaNumber = store.getIdUtils().getLocalSchemaId(table.getId());
                    int tableNumber = store.getIdUtils().getLocalTableId(table.getId());
                    int columnId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                    Vector constraint = new Vector(columnId, sketch);
                    qGramVectors.add(constraint);
                }
            } catch (Exception e) {
                LoggerFactory.getLogger(CreateQGramSketchApp.class).error("Processing {} failed.", table, e);
            }
        }
        return qGramVectors;
    }

    /**
     * Profile all tables of a {@link Schema} for q-gram signatures (min-hash signatures).
     *
     * @param store         within which the {@code schema} resides
     * @param schema        the {@link Schema} whose (CSV) tables should be profiled
     * @param numDimensions number of min-hash dimensions
     * @param seed          to create random transformation matrix from the sketch space to the q-gram vector space
     * @param q             the size of the q-grams
     * @return the q-gram {@link Signature}s
     */
    public static Collection<Signature> profileQGramSignatures(
            MetadataStore store,
            Schema schema,
            int numDimensions,
            int seed,
            int q) {

        List<Signature> qGramSignatures = new ArrayList<>();

        // Initialize the random hash functions.
        Random random = new Random(seed);
        List<ToIntFunction<byte[]>> hashFunctions = new ArrayList<>(numDimensions);
        for (int i = 0; i < numDimensions; i++) {
            HashFunction hashFunction = Hashing.murmur3_32(random.nextInt());
            hashFunctions.add(qGram -> hashFunction.hashBytes(qGram).asInt());
        }

        // Go over the files and create the signatures.
        for (Table table : schema.getTables()) {
            // Get the CSV file location.
            Location location = table.getLocation();
            if (!(location instanceof CsvFileLocation)) {
                LoggerFactory.getLogger(CreateQGramSketchApp.class).error(
                        "Cannot process {} at {}. Only CSV files are supported. Skipping...", table, location
                );
                continue;
            }
            CsvFileLocation csvFileLocation = (CsvFileLocation) location;


            // Prepare the q-gram signature calculation.
            List<int[]> minHashes = new ArrayList<>();
            for (Column column : table.getColumns()) {
                int[] signature = new int[numDimensions];
                Arrays.fill(signature, Integer.MAX_VALUE);
                minHashes.add(signature);
            }
            byte[] qGram = new byte[q * 2]; // UTF-16 encoded q-grams.
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

                // Go over the file and collect the q-grams.
                if (csvFileLocation.getHasHeader()) bufferedReader.readLine();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] fields = csvParser.parseLine(line);
                    for (int fieldIndex = 0; fieldIndex < Math.max(fields.length, minHashes.size()); fieldIndex++) {
                        String field = fields[fieldIndex];
                        if (field == null || field.isEmpty()) continue;

                        // Create the q-grams.
                        for (int start = 1 - q; start < field.length(); start++) {
                            // Assemble the q-gram.
                            for (int offset = 0; offset < q; offset++) {
                                int pos = start + offset;
                                char c = pos < 0 || pos >= field.length() ? '\0' : field.charAt(pos);
                                qGram[2 * offset] = (byte) (c >>> 8);
                                qGram[2 * offset + 1] = (byte) c;
                            }

                            // Update the min-hash signature.
                            int[] columnMinHashes = minHashes.get(fieldIndex);
                            for (int i = 0; i < hashFunctions.size(); i++) {
                                int hash = hashFunctions.get(i).applyAsInt(qGram);
                                if (hash < columnMinHashes[i]) columnMinHashes[i] = hash;
                            }

                        }
                    }
                }

                // Create the sketches.
                for (int columnIndex = 0; columnIndex < minHashes.size(); columnIndex++) {
                    int[] columnMinHashes = minHashes.get(columnIndex);
                    int schemaNumber = store.getIdUtils().getLocalSchemaId(table.getId());
                    int tableNumber = store.getIdUtils().getLocalTableId(table.getId());
                    int columnId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                    Signature signature = new Signature(columnId, columnMinHashes);
                    qGramSignatures.add(signature);
                }
            } catch (Exception e) {
                LoggerFactory.getLogger(CreateQGramSketchApp.class).error("Processing {} failed.", table, e);
            }
        }
        return qGramSignatures;
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
        CreateQGramSketchApp.Parameters parameters = new CreateQGramSketchApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new CreateQGramSketchApp(parameters).run();
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

        @Parameter(names = "--sketch-dimensions", description = "dimensionality of the Q-gram sketches")
        private int numSketchDimensions = 150;

        @Parameter(names = "--qgram-dimensions", description = "dimensionality of the Q-gram vectors")
        private int numQGramDimensions = 8192;

        @Parameter(names = {"-q", "--q"}, description = "size of the q-grams")
        private int q = 3;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

    }

}
