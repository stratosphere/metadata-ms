/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.metadata_store.benchmark;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.constraints.impl.DistinctValueCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.UniqueColumnCombination;
import de.hpi.isg.metadata_store.domain.factories.MetadataStoreFactory;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * A little test set to quantify the performance of constraint insertions into a metadata store.
 *
 * @author Sebastian Kruse
 */
public class ConstraintInsertPerfomanceBenchmark {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintInsertPerfomanceBenchmark.class);

    private File createTempFile(String suffix) throws IOException {
        File file = File.createTempFile("metadatastore", suffix);
        file.deleteOnExit();
        LOGGER.debug("Using temporary file {}.", file.getAbsolutePath());
        return file;
    }

    private Schema createSchema(MetadataStore metadataStore, int numTables, int numColumnsPerTable) {
        Schema schema = metadataStore.addSchema("test-schema", null, new DefaultLocation());
        for (int tableNum = 0; tableNum < numTables; tableNum++) {
            Table table = schema.addTable(metadataStore, String.format("test-table-%04d", tableNum), null,
                    new DefaultLocation());
            for (int columnNum = 0; columnNum < numColumnsPerTable; columnNum++) {
                table.addColumn(metadataStore, String.format("test-column-%04d", columnNum), null, columnNum);
            }
        }
        return schema;
    }

    @Test
    public void testInsertDistinctValueCountsIntoDefaultMetadataStore() throws Exception {

        LOGGER.info("Creating Java-serialized metadata store...");
        File metadataStoreFile = createTempFile("ser");
        MetadataStore metadataStore = MetadataStoreFactory.createAndSaveDefaultMetadataStore(metadataStoreFile);

        LOGGER.info("Creating schema...");
        int numTables = 1000;
        int numColumnsPerTable = 100;
        int numColumns = numTables * numColumnsPerTable;
        Schema schema = createSchema(metadataStore, numTables, numColumnsPerTable);
        metadataStore.flush();

        LOGGER.info("Inserting {} distinct value counts...", numColumns);
        long startTimeGross = System.currentTimeMillis();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(null);
        long startTimeNet = System.currentTimeMillis();
        for (Table table : schema.getTables()) {
            for (Column column : table.getColumns()) {
                DistinctValueCount dvCount = DistinctValueCount.buildAndAddToCollection(new SingleTargetReference(
                        column), constraintCollection, 100);
                constraintCollection.add(dvCount);
            }
        }
        long endTimeNet = System.currentTimeMillis();
        metadataStore.flush();
        long endTimeGross = System.currentTimeMillis();
        double numInsertsPerSecGross = 1000d * numColumns / (endTimeGross - startTimeGross);
        double numInsertsPerSecNet = 1000d * numColumns / (endTimeNet - startTimeNet);
        LOGGER.info("[gross] Inserted in {} ms ({} inserts/s)", endTimeGross - startTimeGross, numInsertsPerSecGross);
        LOGGER.info("[net]   Inserted in {} ms ({} inserts/s)", endTimeNet - startTimeNet, numInsertsPerSecNet);
        LOGGER.info("File size: {} MB", metadataStoreFile.length() / (1024 * 1024));
    }

    @Test
    public void testInsertDistinctValueCountsIntoRDBMSMetadataStore() throws Exception {

        LOGGER.info("Creating RDBMS metadata store...");
        File metadataStoreFile = createTempFile("sqlite");
        Connection connection = MetadataStoreFactory.createSQLiteConnection(metadataStoreFile);
        MetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

        LOGGER.info("Creating schema...");
        int numTables = 1000;
        int numColumnsPerTable = 100;
        int numColumns = numTables * numColumnsPerTable;
        Schema schema = createSchema(metadataStore, numTables, numColumnsPerTable);
        metadataStore.flush();

        LOGGER.info("Collecting all columns...", numColumns);
        List<Column> allColumns = new ArrayList<>();
        for (Table table : schema.getTables()) {
            for (Column column : table.getColumns()) {
                allColumns.add(column);
            }
        }
        LOGGER.info("Inserting {} distinct value counts...", numColumns);
        long startTimeGross = System.currentTimeMillis();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(null);
        long startTimeNet = System.currentTimeMillis();
        for (Column column : allColumns) {
            DistinctValueCount dvCount = DistinctValueCount.buildAndAddToCollection(new SingleTargetReference(column),
                    constraintCollection, 100);
            constraintCollection.add(dvCount);
        }
        long endTimeNet = System.currentTimeMillis();
        metadataStore.flush();
        long endTimeGross = System.currentTimeMillis();
        double numInsertsPerSecGross = 1000d * numColumns / (endTimeGross - startTimeGross);
        double numInsertsPerSecNet = 1000d * numColumns / (endTimeNet - startTimeNet);
        LOGGER.info("[gross] Inserted in {} ms ({} inserts/s)", endTimeGross - startTimeGross, numInsertsPerSecGross);
        LOGGER.info("[net]   Inserted in {} ms ({} inserts/s)", endTimeNet - startTimeNet, numInsertsPerSecNet);
        LOGGER.info("File size: {} MB", metadataStoreFile.length() / (1024 * 1024));
    }

    @Test
    public void testInsertInclusionDependenciesIntoDefaultMetadataStore() throws Exception {

        LOGGER.info("Creating Java-serialized metadata store...");
        File metadataStoreFile = createTempFile("ser");
        MetadataStore metadataStore = MetadataStoreFactory.createAndSaveDefaultMetadataStore(metadataStoreFile);

        LOGGER.info("Creating schema...");
        int numTables = 1000;
        int numColumnsPerTable = 100;
        int numColumns = numTables * numColumnsPerTable;
        Schema schema = createSchema(metadataStore, numTables, numColumnsPerTable);
        metadataStore.flush();

        LOGGER.info("Generating INDs...");
        int numDesiredInds = 100000;
        double indProbablity = numDesiredInds / Math.pow(numTables * numColumnsPerTable, 2);
        // Boost probablity to speed up generation.
        indProbablity = Math.sqrt(indProbablity);

        Collection<Column[]> inclusionDependencies = new LinkedList<Column[]>();
        Random random = new Random();
        OuterLoop: for (final Table table1 : schema.getTables()) {
            for (final Table table2 : schema.getTables()) {
                for (final Column column1 : table1.getColumns()) {
                    for (final Column column2 : table2.getColumns()) {
                        if (column1 != column2 && random.nextDouble() <= indProbablity) {
                            inclusionDependencies.add(new Column[] { column1, column2 });
                            if (inclusionDependencies.size() >= numDesiredInds) {
                                break OuterLoop;
                            }
                        }
                    }
                }
            }
        }

        LOGGER.info("Inserting the {} generated INDs...", inclusionDependencies.size());
        long startTimeGross = System.currentTimeMillis();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(null);
        long startTimeNet = System.currentTimeMillis();
        for (Column[] columnPair : inclusionDependencies) {
            Collection<Column> dependentColumns = Collections.singleton(columnPair[0]);
            Collection<Column> referencedColumns = Collections.singletonList(columnPair[1]);
            final InclusionDependency.Reference reference = new InclusionDependency.Reference(
                    dependentColumns.toArray(new Column[dependentColumns.size()]),
                    referencedColumns.toArray(new Column[referencedColumns.size()]));
            InclusionDependency.buildAndAddToCollection(reference, constraintCollection);
        }
        long endTimeNet = System.currentTimeMillis();
        metadataStore.flush();
        long endTimeGross = System.currentTimeMillis();
        double numInsertsPerSecGross = 1000d * numColumns / (endTimeGross - startTimeGross);
        double numInsertsPerSecNet = 1000d * numColumns / (endTimeNet - startTimeNet);
        LOGGER.info("[gross] Inserted in {} ms ({} inserts/s)", endTimeGross - startTimeGross, numInsertsPerSecGross);
        LOGGER.info("[net]   Inserted in {} ms ({} inserts/s)", endTimeNet - startTimeNet, numInsertsPerSecNet);
        LOGGER.info("File size: {} MB", metadataStoreFile.length() / (1024 * 1024));
    }

    @Test
    public void testInsertInclusionDependenciesIntoRDBMSMetadataStore() throws Exception {

        LOGGER.info("Creating RDBMS metadata store...");
        File metadataStoreFile = createTempFile("sqlite");
        Connection connection = MetadataStoreFactory.createSQLiteConnection(metadataStoreFile);
        MetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

        LOGGER.info("Creating schema...");
        int numTables = 1000;
        int numColumnsPerTable = 100;
        int numColumns = numTables * numColumnsPerTable;
        Schema schema = createSchema(metadataStore, numTables, numColumnsPerTable);
        metadataStore.flush();

        LOGGER.info("Generating INDs...");
        int numDesiredInds = 100000;
        double indProbablity = numDesiredInds / Math.pow(numTables * numColumnsPerTable, 2);
        // Boost probablity to speed up generation.
        indProbablity = Math.sqrt(indProbablity);

        Collection<Column[]> inclusionDependencies = new LinkedList<Column[]>();
        Random random = new Random();
        OuterLoop: for (final Table table1 : schema.getTables()) {
            for (final Table table2 : schema.getTables()) {
                for (final Column column1 : table1.getColumns()) {
                    for (final Column column2 : table2.getColumns()) {
                        if (column1 != column2 && random.nextDouble() <= indProbablity) {
                            inclusionDependencies.add(new Column[] { column1, column2 });
                            if (inclusionDependencies.size() >= numDesiredInds) {
                                break OuterLoop;
                            }
                        }
                    }
                }
            }
        }

        LOGGER.info("Inserting the {} generated INDs...", inclusionDependencies.size());
        long startTimeGross = System.currentTimeMillis();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(null);
        long startTimeNet = System.currentTimeMillis();
        for (Column[] columnPair : inclusionDependencies) {
            Collection<Column> dependentColumns = Collections.singleton(columnPair[0]);
            Collection<Column> referencedColumns = Collections.singletonList(columnPair[1]);
            final InclusionDependency.Reference reference = new InclusionDependency.Reference(
                    dependentColumns.toArray(new Column[dependentColumns.size()]),
                    referencedColumns.toArray(new Column[referencedColumns.size()]));
            InclusionDependency.buildAndAddToCollection(reference, constraintCollection);
        }
        long endTimeNet = System.currentTimeMillis();
        metadataStore.flush();
        long endTimeGross = System.currentTimeMillis();
        double numInsertsPerSecGross = 1000d * numColumns / (endTimeGross - startTimeGross);
        double numInsertsPerSecNet = 1000d * numColumns / (endTimeNet - startTimeNet);
        LOGGER.info("[gross] Inserted in {} ms ({} inserts/s)", endTimeGross - startTimeGross, numInsertsPerSecGross);
        LOGGER.info("[net]   Inserted in {} ms ({} inserts/s)", endTimeNet - startTimeNet, numInsertsPerSecNet);
        LOGGER.info("File size: {} MB", metadataStoreFile.length() / (1024 * 1024));
    }

    @Test
    public void testInsertUniqueColumnCombinationsIntoDefaultMetadataStore() throws Exception {

        LOGGER.info("Creating Java-serialized metadata store...");
        File metadataStoreFile = createTempFile("ser");
        MetadataStore metadataStore = MetadataStoreFactory.createAndSaveDefaultMetadataStore(metadataStoreFile);

        LOGGER.info("Creating schema...");
        int numTables = 1000;
        int numColumnsPerTable = 100;
        int numColumns = numTables * numColumnsPerTable;
        Schema schema = createSchema(metadataStore, numTables, numColumnsPerTable);
        metadataStore.flush();

        LOGGER.info("Generating UCCs...");
        int numDesiredInds = 100000;
        double indProbablity = numDesiredInds / Math.pow(numTables * numColumnsPerTable, 2);
        // Boost probablity to speed up generation.
        indProbablity = Math.sqrt(indProbablity);

        Collection<Column[]> inclusionDependencies = new LinkedList<Column[]>();
        Random random = new Random();
        OuterLoop: for (final Table table1 : schema.getTables()) {
            for (final Table table2 : schema.getTables()) {
                for (final Column column1 : table1.getColumns()) {
                    for (final Column column2 : table2.getColumns()) {
                        if (column1 != column2 && random.nextDouble() <= indProbablity) {
                            inclusionDependencies.add(new Column[] { column1, column2 });
                            if (inclusionDependencies.size() >= numDesiredInds) {
                                break OuterLoop;
                            }
                        }
                    }
                }
            }
        }

        LOGGER.info("Inserting the {} generated UCCs...", inclusionDependencies.size());
        long startTimeGross = System.currentTimeMillis();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(null);
        long startTimeNet = System.currentTimeMillis();
        for (Column[] columnPair : inclusionDependencies) {
            Collection<Column> uniqueColumns = Collections.singleton(columnPair[0]);
            final UniqueColumnCombination.Reference reference = new UniqueColumnCombination.Reference(
                    uniqueColumns.toArray(new Column[uniqueColumns.size()]));
            UniqueColumnCombination.buildAndAddToCollection(reference, constraintCollection);
        }
        long endTimeNet = System.currentTimeMillis();
        metadataStore.flush();
        long endTimeGross = System.currentTimeMillis();
        double numInsertsPerSecGross = 1000d * numColumns / (endTimeGross - startTimeGross);
        double numInsertsPerSecNet = 1000d * numColumns / (endTimeNet - startTimeNet);
        LOGGER.info("[gross] Inserted in {} ms ({} inserts/s)", endTimeGross - startTimeGross, numInsertsPerSecGross);
        LOGGER.info("[net]   Inserted in {} ms ({} inserts/s)", endTimeNet - startTimeNet, numInsertsPerSecNet);
        LOGGER.info("File size: {} MB", metadataStoreFile.length() / (1024 * 1024));

    }

    @Test
    public void testInsertUniqueColumnCombinationsIntoRDBMSMetadataStore() throws Exception {

        LOGGER.info("Creating RDBMS metadata store...");
        File metadataStoreFile = createTempFile("sqlite");
        Connection connection = MetadataStoreFactory.createSQLiteConnection(metadataStoreFile);
        MetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

        LOGGER.info("Creating schema...");
        int numTables = 1000;
        int numColumnsPerTable = 100;
        int numColumns = numTables * numColumnsPerTable;
        Schema schema = createSchema(metadataStore, numTables, numColumnsPerTable);
        metadataStore.flush();

        LOGGER.info("Generating UCCs...");
        int numDesiredInds = 100000;
        double indProbablity = numDesiredInds / Math.pow(numTables * numColumnsPerTable, 2);
        // Boost probablity to speed up generation.
        indProbablity = Math.sqrt(indProbablity);

        Collection<Column[]> inclusionDependencies = new LinkedList<Column[]>();
        Random random = new Random();
        OuterLoop: for (final Table table1 : schema.getTables()) {
            for (final Table table2 : schema.getTables()) {
                for (final Column column1 : table1.getColumns()) {
                    for (final Column column2 : table2.getColumns()) {
                        if (column1 != column2 && random.nextDouble() <= indProbablity) {
                            inclusionDependencies.add(new Column[] { column1, column2 });
                            if (inclusionDependencies.size() >= numDesiredInds) {
                                break OuterLoop;
                            }
                        }
                    }
                }
            }
        }

        LOGGER.info("Inserting the {} generated UCCs...", inclusionDependencies.size());
        long startTimeGross = System.currentTimeMillis();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(null);
        long startTimeNet = System.currentTimeMillis();
        for (Column[] columnPair : inclusionDependencies) {
            Collection<Column> uniqueColumns = Collections.singleton(columnPair[0]);
            final UniqueColumnCombination.Reference reference = new UniqueColumnCombination.Reference(
                    uniqueColumns.toArray(new Column[uniqueColumns.size()]));
            UniqueColumnCombination.buildAndAddToCollection(reference, constraintCollection);
        }
        long endTimeNet = System.currentTimeMillis();
        metadataStore.flush();
        long endTimeGross = System.currentTimeMillis();
        double numInsertsPerSecGross = 1000d * numColumns / (endTimeGross - startTimeGross);
        double numInsertsPerSecNet = 1000d * numColumns / (endTimeNet - startTimeNet);
        LOGGER.info("[gross] Inserted in {} ms ({} inserts/s)", endTimeGross - startTimeGross, numInsertsPerSecGross);
        LOGGER.info("[net]   Inserted in {} ms ({} inserts/s)", endTimeNet - startTimeNet, numInsertsPerSecNet);
        LOGGER.info("File size: {} MB", metadataStoreFile.length() / (1024 * 1024));

    }
}