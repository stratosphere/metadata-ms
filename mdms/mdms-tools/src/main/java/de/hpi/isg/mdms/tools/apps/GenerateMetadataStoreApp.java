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
import de.hpi.isg.mdms.Encoding;
import de.hpi.isg.mdms.clients.apps.AppTemplate;
import de.hpi.isg.mdms.clients.location.CsvFileLocation;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The purpose of this app is to generate a {@link MetadataStore} with synthetic contents.
 *
 * @author Sebastian Kruse
 */
public class GenerateMetadataStoreApp extends AppTemplate<GenerateMetadataStoreApp.Parameters> {

    /**
     * Provides randomness for the generation of {@link ConstraintCollection}s.
     */
    private final Random random = new Random();

    private static final String[] TYPE_CONSTRAINT_TYPES = {
            "INT", "REAL", "DOUBLE", "FLOAT", "BOOLEAN", "CHAR[32]", "CHAR[100]", "VARCHAR[32]", "VARCHAR[100]",
            "TEXT", "STRING", "BLOB", "CLOB"
    };

    private static final String[] SUBTYPES = {"TIME", "DATE", "DATETIME", "JSON", "XML", "CSV"};

    /**
     * Creates a new instance of this class.
     *
     * @see AppTemplate#AppTemplate(Object)
     */
    public GenerateMetadataStoreApp(GenerateMetadataStoreApp.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Create the store.
        MetadataStore store = MetadataStoreUtil.createMetadataStore(
                this.parameters.metadataStoreParameters,
                this.parameters.isOverwrite,
                this.parameters.numTableBitsInIds,
                this.parameters.numColumnBitsInIds);
        this.logger.info("Created {} at {}..\n", store, this.parameters.metadataStoreParameters.metadataStore);

        // Create the schemata.
        for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
            Schema schema = store.addSchema(String.format("schema-%04x", schemaNumber), null, null);
            assert schema.getId() == store.getIdUtils().createGlobalId(schemaNumber);
            for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                CsvFileLocation location = new CsvFileLocation();
                location.setEncoding(Encoding.DEFAULT_ENCODING);
                location.setHasHeader(false);
                location.setFieldSeparator('\t');
                location.setQuoteChar('\0');
                location.setNullString("\\N");
                location.setPath(String.format("hdfs://data/schema-%04x/table-%04x.csv", schemaNumber, tableNumber));
                Table table = schema.addTable(store, String.format("table-%04x", tableNumber), null, location);
                assert table.getId() == store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                for (int columnIndex = 0; columnIndex < this.parameters.numColumns; columnIndex++) {
                    Column column = table.addColumn(store, String.format("column-%04x", columnIndex), null, columnIndex);
                    assert column.getId() == store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                }
            }
        }
        store.flush();

        // Create the INDs.
        if (this.parameters.numInds >= 0) {
            ConstraintCollection<InclusionDependency> inds = store.createConstraintCollection(
                    String.format("%,d randomly generated unary INDs.", this.parameters.numInds),
                    InclusionDependency.class,
                    store.getSchemas().toArray(new Target[0])
            );
            for (int i = 0; i < this.parameters.numInds; i++) {
                inds.add(new InclusionDependency(this.generateRandomColumnId(store), this.generateRandomColumnId(store)));
            }
            this.logger.info("Added {} INDs.", this.parameters.numInds);
        }

        // Create the UCCs.
        if (this.parameters.numUccs >= 0) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<UniqueColumnCombination> uccs = store.createConstraintCollection(
                            String.format("%,d randomly generated UCCs for %s.%s.", this.parameters.numUccs, table.getSchema().getName(), table.getName()),
                            UniqueColumnCombination.class,
                            table
                    );
                    for (int i = 0; i < this.parameters.numUccs; i++) {
                        int[] randomColumnIds = this.generateRandomColumnCombinationIds(store, table.getId());
                        uccs.add(new UniqueColumnCombination(randomColumnIds));
                    }
                }
            }
        }

        // Create the FDs.
        if (this.parameters.numFds >= 0) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<FunctionalDependency> fds = store.createConstraintCollection(
                            String.format("%,d randomly generated FDs for %s.%s.", this.parameters.numFds, table.getSchema().getName(), table.getName()),
                            FunctionalDependency.class,
                            table
                    );
                    for (int i = 0; i < this.parameters.numFds; i++) {
                        // Generate random columns.
                        int[] randomColumnIds = this.generateRandomColumnCombinationIds(store, table.getId());

                        // Designate one of the columns as RHS for the FD.
                        int rhsIndex = this.random.nextInt(randomColumnIds.length);
                        int[] lhs = new int[randomColumnIds.length - 1];
                        System.arraycopy(randomColumnIds, 0, lhs, 0, rhsIndex);
                        System.arraycopy(randomColumnIds, rhsIndex + 1, lhs, rhsIndex, lhs.length - rhsIndex);

                        // Add the FD.
                        fds.add(new FunctionalDependency(lhs, randomColumnIds[rhsIndex]));
                    }
                }
            }
        }

        // Create the ODs.
        if (this.parameters.numOds >= 0) {
            Validate.isTrue(this.parameters.numColumns > 2, "Need at least 2 columns per table to create order dependencies.");
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<OrderDependency> ods = store.createConstraintCollection(
                            String.format("%,d randomly generated ODs for %s.%s.", this.parameters.numOds, table.getSchema().getName(), table.getName()),
                            OrderDependency.class,
                            table
                    );
                    for (int i = 0; i < this.parameters.numOds; i++) {
                        // Generate random columns.
                        int[] randomColumnIds = this.generateRandomColumnCombinationIds(store, table.getId(), 2);
                        this.shuffle(randomColumnIds);

                        // Split the columns in two parts, using one as LHS the other as RHS.
                        int rhsStartIndex = 1 + this.random.nextInt(randomColumnIds.length - 1);
                        ods.add(new OrderDependency(
                                Arrays.copyOfRange(randomColumnIds, 0, rhsStartIndex),
                                Arrays.copyOfRange(randomColumnIds, rhsStartIndex, randomColumnIds.length)
                        ));
                    }
                }
            }
        }

        // Generate type constraints.
        if (this.parameters.isGenerateTypeConstraints) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<TypeConstraint> typeConstraints = store.createConstraintCollection(
                            String.format("Randomly generated type constraints for %s.%s.", table.getSchema().getName(), table.getName()),
                            TypeConstraint.class,
                            table
                    );
                    for (int columnIndex = 0; columnIndex < this.parameters.numColumns; columnIndex++) {
                        String type = TYPE_CONSTRAINT_TYPES[this.random.nextInt(TYPE_CONSTRAINT_TYPES.length)];
                        typeConstraints.add(new TypeConstraint(
                                store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex), type
                        ));
                    }
                }
            }
        }

        // Generate tuple counts.
        if (this.parameters.isGenerateTupleCounts) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                int schemaId = store.getIdUtils().createGlobalId(schemaNumber);
                Schema schema = store.getSchemaById(schemaId);
                ConstraintCollection<TupleCount> tupleCounts = store.createConstraintCollection(
                        String.format("Randomly generated tuple counts for %s.", schema.getName()),
                        TupleCount.class,
                        schema
                );
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    tupleCounts.add(new TupleCount(tableId, this.random.nextInt(1_000_000)));
                }
            }
        }

        // Generate distinct value counts.
        if (this.parameters.isGenerateDistinctValueCount) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<DistinctValueCount> dvcs = store.createConstraintCollection(
                            String.format("Randomly generated distinct value counts for %s.%s.", table.getSchema().getName(), table.getName()),
                            DistinctValueCount.class,
                            table
                    );
                    for (int columnIndex = 0; columnIndex < this.parameters.numColumns; columnIndex++) {
                        int columnId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                        dvcs.add(new DistinctValueCount(columnId, this.random.nextInt(1_000_000)));
                    }
                }
            }
        }

        // Generate column statistics.
        if (this.parameters.isGenerateColumnStatistics) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<ColumnStatistics> statistics = store.createConstraintCollection(
                            String.format("Randomly generated column statistics for %s.%s.", table.getSchema().getName(), table.getName()),
                            ColumnStatistics.class,
                            table
                    );
                    for (int columnIndex = 0; columnIndex < this.parameters.numColumns; columnIndex++) {
                        int columnId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                        ColumnStatistics columnStatistics = new ColumnStatistics(columnId);
                        columnStatistics.setEntropy(Math.abs(this.random.nextGaussian()));
                        columnStatistics.setFillStatus(this.random.nextDouble());
                        columnStatistics.setNumNulls(this.random.nextInt(1_000_000));
                        columnStatistics.setNumDistinctValues(this.random.nextInt(1_000_000));
                        columnStatistics.setUniqueness(this.random.nextDouble());
                        List<ColumnStatistics.ValueOccurrence> topKFrequentValues = new ArrayList<>(10);
                        for (int i = 0; i < 10; i++) {
                            String string = this.generateRandomString(0.1 + 0.8 * this.random.nextDouble());
                            topKFrequentValues.add(new ColumnStatistics.ValueOccurrence(
                                    string, this.random.nextInt(1_000_000)
                            ));
                        }
                        columnStatistics.setTopKFrequentValues(topKFrequentValues);
                        statistics.add(columnStatistics);
                    }
                }
            }
        }

        // Generate text column statistics.
        if (this.parameters.isGenerateTextColumnStatistics) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<TextColumnStatistics> statistics = store.createConstraintCollection(
                            String.format("Randomly generated text column statistics for %s.%s.", table.getSchema().getName(), table.getName()),
                            TextColumnStatistics.class,
                            table
                    );
                    for (int columnIndex = 0; columnIndex < this.parameters.numColumns; columnIndex++) {
                        int columnId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                        TextColumnStatistics columnStatistics = new TextColumnStatistics(columnId);
                        columnStatistics.setShortestValue(this.generateRandomString(0.1));
                        columnStatistics.setLongestValue(this.generateRandomString(0.9));
                        columnStatistics.setSubtype(SUBTYPES[this.random.nextInt(SUBTYPES.length)]);
                        columnStatistics.setMinValue(this.generateRandomString(0.1 + 0.8 * this.random.nextDouble()));
                        columnStatistics.setMaxValue(this.generateRandomString(0.1 + 0.8 * this.random.nextDouble()));
                        statistics.add(columnStatistics);
                    }
                }
            }
        }

        // Generate number column statistics.
        if (this.parameters.isGenerateNumberColumnStatistics) {
            for (int schemaNumber = 0; schemaNumber < this.parameters.numSchemata; schemaNumber++) {
                for (int tableNumber = 0; tableNumber < this.parameters.numTables; tableNumber++) {
                    int tableId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber);
                    Table table = (Table) store.getTargetById(tableId);
                    ConstraintCollection<NumberColumnStatistics> statistics = store.createConstraintCollection(
                            String.format("Randomly generated number column statistics for %s.%s.", table.getSchema().getName(), table.getName()),
                            NumberColumnStatistics.class,
                            table
                    );
                    for (int columnIndex = 0; columnIndex < this.parameters.numColumns; columnIndex++) {
                        int columnId = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
                        NumberColumnStatistics columnStatistics = new NumberColumnStatistics(columnId);
                        columnStatistics.setMinValue(-Math.abs(this.random.nextGaussian()));
                        columnStatistics.setMaxValue(Math.abs(this.random.nextGaussian()));
                        columnStatistics.setAverage((columnStatistics.getMinValue() + columnStatistics.getMaxValue()) / 2);
                        columnStatistics.setStandardDeviation(Math.abs(this.random.nextGaussian()));
                        statistics.add(columnStatistics);
                    }
                }
            }
        }

        // Make sure that any changes are committed.
        store.flush();
        this.logger.info("Created {} at {}..\n", store, this.parameters.metadataStoreParameters.metadataStore);
    }

    /**
     * Obtain a random {@link Schema} using {@link #random}.
     *
     * @param store from which the {@link Schema} is drawn; should be initialized according to the {@link #parameters}
     * @return the {@link Schema}
     */
    private final Schema generateRandomSchema(MetadataStore store) {
        int schemaNumber = this.random.nextInt(this.parameters.numSchemata);
        int schemaId = store.getIdUtils().createGlobalId(schemaNumber);
        return store.getSchemaById(schemaId);
    }

    /**
     * Obtain a random {@link Column} using {@link #random}.
     *
     * @param store from which the {@link Column} is drawn; should be initialized according to the {@link #parameters}
     * @return the {@link Column}
     */
    private final Column generateRandomColumn(MetadataStore store) {
        return (Column) store.getTargetById(this.generateRandomColumnId(store));
    }

    /**
     * Obtain the ID of a random {@link Column} using {@link #random}.
     *
     * @param store from which the {@link Column} is drawn; should be initialized according to the {@link #parameters}
     * @return the {@link Column}
     */
    private final int generateRandomColumnId(MetadataStore store) {
        int schemaNumber = this.random.nextInt(this.parameters.numSchemata);
        int tableNumber = this.random.nextInt(this.parameters.numTables);
        int columnIndex = this.random.nextInt(this.parameters.numColumns);
        return store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
    }

    /**
     * Obtain the IDs of {@link Column}s picked randomly from a {@link Table} with the given {@code tableId}.
     *
     * @param store   that contains the {@link Table}
     * @param tableId the ID of a the {@link Table}, from which the {@link Column}s should be picked; should conform to {@link #parameters}
     * @return a sorted array of randomly picked, distinct {@link Column} IDs
     */
    private final int[] generateRandomColumnCombinationIds(MetadataStore store, int tableId) {
        return this.generateRandomColumnCombinationIds(store, tableId, 1);
    }

    /**
     * Obtain the IDs of {@link Column}s picked randomly from a {@link Table} with the given {@code tableId}.
     *
     * @param store    that contains the {@link Table}
     * @param tableId  the ID of a the {@link Table}, from which the {@link Column}s should be picked; should conform to {@link #parameters}
     * @param minArity the minimum arity of the picked {@link Column}s
     * @return a sorted array of randomly picked, distinct {@link Column} IDs
     */
    private final int[] generateRandomColumnCombinationIds(MetadataStore store, int tableId, int minArity) {
        // Choose an arity.
        int arity = minArity + this.random.nextInt(this.parameters.numColumns - minArity);
        int[] columnIds = new int[arity];

        // Use reservoir sampling to select random columns.
        int schemaNumber = store.getIdUtils().getLocalSchemaId(tableId);
        int tableNumber = store.getIdUtils().getLocalTableId(tableId);
        int columnIndex = 0;
        while (columnIndex < arity) {
            columnIds[columnIndex] = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
            columnIndex++;
        }
        while (columnIndex < this.parameters.numColumns) {
            int index = this.random.nextInt(columnIndex);
            if (index < arity)
                columnIds[index] = store.getIdUtils().createGlobalId(schemaNumber, tableNumber, columnIndex);
            columnIndex++;
        }

        // Sort and return the column IDs.
        Arrays.sort(columnIds);
        return columnIds;
    }

    /**
     * Generate a random {@link String} of the given {@code length} consisting of characters with code points between
     * 32 and 126.
     *
     * @param length of the {@link String} to be generated
     * @return the generated {@link String}
     */
    private String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) (32 + this.random.nextInt(127 - 32)));
        }
        return sb.toString();
    }

    /**
     * Generate a random {@link String} consisting of characters with code points between
     * 32 and 126. The length of the {@link String} is determined by successive coin flips.
     *
     * @param p the probability of adding another character to the {@link String} every time a character has been added
     * @return the generated {@link String}
     */
    private String generateRandomString(double p) {
        StringBuilder sb = new StringBuilder();
        do {
            sb.append((char) (32 + this.random.nextInt(127 - 32)));
        } while (this.random.nextDouble() < p);
        return sb.toString();
    }

    /**
     * Shuffle the given {@code array} using {@link #random}.
     *
     * @param array should be shuffled
     */
    private void shuffle(int[] array) {
        // Strategy: swap every element with a random element.
        for (int i = 0; i < array.length; i++) {
            int j = this.random.nextInt(array.length);
            if (i != j) {
                array[i] ^= array[j];
                array[j] ^= array[i];
                array[i] ^= array[j];
            }
        }
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    public static void main(final String[] args) throws Exception {
        GenerateMetadataStoreApp.Parameters parameters = new GenerateMetadataStoreApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new GenerateMetadataStoreApp(parameters).run();
    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    @SuppressWarnings("serial")
    public static class Parameters implements Serializable {

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @Parameter(names = "--overwrite")
        public boolean isOverwrite = false;

        @Parameter(names = MetadataStoreParameters.NUM_TABLE_BITS_IN_IDS,
                description = "number of bits (of 32 overall bits) to use for encoding table IDs")
        public int numTableBitsInIds = IdUtils.DEFAULT_NUM_TABLE_BITS;

        @Parameter(names = MetadataStoreParameters.NUM_COLUMN_BITS_IN_IDS,
                description = "number of bits (of 32 overall bits) to use for encoding table IDs")
        public int numColumnBitsInIds = IdUtils.DEFAULT_NUM_COLUMN_BITS;

        @Parameter(names = "--schemata", description = "the number of schemata to generate")
        public int numSchemata = 1;

        @Parameter(names = "--tables", description = "the number of tables per schema to generate")
        public int numTables = 100;

        @Parameter(names = "--columns", description = "the number of columns per table to generate")
        public int numColumns = 10;

        @Parameter(names = "--inds", description = "if non-negative, a collection of that many INDs in generated")
        public int numInds = -1;

        @Parameter(names = "--uccs", description = "if non-negative, a collection of that many UCCs in generated for every table")
        public int numUccs = -1;

        @Parameter(names = "--fds", description = "if non-negative, a collection of that many FDs in generated for every table")
        public int numFds = -1;

        @Parameter(names = "--ods", description = "if non-negative, a collection of that many ODs in generated for every table")
        public int numOds = -1;

        @Parameter(names = "--statistics", description = "generate column statistics for every column")
        public boolean isGenerateColumnStatistics = false;

        @Parameter(names = "--dvcs", description = "generate distinct value counts for every column")
        public boolean isGenerateDistinctValueCount = false;

        @Parameter(names = "--text-statistics", description = "generate text column statistics for every column")
        public boolean isGenerateTextColumnStatistics = false;

        @Parameter(names = "--number-statistics", description = "generate number column statistics for every column")
        public boolean isGenerateNumberColumnStatistics = false;

        @Parameter(names = "--tuple-counts", description = "generate tuple counts for every table")
        public boolean isGenerateTupleCounts = false;

        @Parameter(names = "--type-counts", description = "generate type constraints for every column")
        public boolean isGenerateTypeConstraints = false;

    }

}
