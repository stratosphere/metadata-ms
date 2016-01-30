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
package de.hpi.isg.mdms.flink.location;

import de.hpi.isg.mdms.clients.parameters.CsvParameters;
import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.flink.readwrite.MultiFileTextInputFormat;
import de.hpi.isg.mdms.flink.functions.ParseCsvRows;
import de.hpi.isg.mdms.flink.functions.SplitMergedCsvRows;
import de.hpi.isg.mdms.flink.util.CsvParser;
import de.hpi.isg.mdms.flink.util.FileUtils;
import de.hpi.isg.mdms.flink.util.PlanBuildingUtils;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.util.CollectionUtils;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.*;

/**
 * Describes the location of a CSV file.
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class MergedCsvFilePartitionLocation extends DefaultLocation implements CellLocation, TupleLocation {

    public static final String PARTITION_ID = "PARTITION";

    public int getPartitionId() {
        return Integer.parseInt(get(PARTITION_ID));
    }

    public void setPartitionId(int partitionId) {
        set(PARTITION_ID, String.valueOf(partitionId));
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSourceBuilder<Table, MergedCsvFilePartitionLocation, Tuple2<Integer, String>> getCellDataSourceBuilder() {
        return CellDataSourceBuilder.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSourceBuilder<Table, MergedCsvFilePartitionLocation, Tuple> getTupleDataSourceBuilder() {
        // TODO
        throw new NotImplementedException();
    }

    public static class CellDataSourceBuilder implements
            DataSourceBuilder<Table, MergedCsvFilePartitionLocation, Tuple2<Integer, String>> {

        static final CellDataSourceBuilder INSTANCE = new CellDataSourceBuilder();

        @Override
        public DataSet<Tuple2<Integer, String>> buildDataSource(ExecutionEnvironment env, Collection<Table> allTables,
                                                                MetadataStore metadataStore, boolean isAllowingEmptyFields) {

            Map<Path, List<Table>> tablesByCsvParameters = partitionTablesByFile(allTables);
            Collection<DataSet<Tuple2<Integer, String>>> cellDataSets = new LinkedList<>();

            for (Map.Entry<Path, List<Table>> partitionEntry : tablesByCsvParameters.entrySet()) {

                // Create a mapping from in-file table IDs to metadata store IDs.
                Path path = partitionEntry.getKey();
                List<Table> tables = partitionEntry.getValue();
                MergedCsvFileLocation schemaLocation = (MergedCsvFileLocation) CollectionUtils.getAny(tables)
                        .getSchema().getLocation();
                CsvParameters csvParameters = schemaLocation.getCsvParameters();
                Int2IntMap idDictionary = createIdDictionary(tables, metadataStore.getIdUtils());
                Object2IntMap<String> pathIds = new Object2IntOpenHashMap<>();
                try {
                    for (Path actualFilePath : FileUtils.gatherFiles(1, path)) {
                        pathIds.put(actualFilePath.toString(), 0);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Could not determine input files.", e);
                }
                final MultiFileTextInputFormat.ListBasedFileIdRetriever fileIdRetriever = new MultiFileTextInputFormat.ListBasedFileIdRetriever(
                        pathIds);
                MultiFileTextInputFormat inputFormat;
                inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever);
                inputFormat.setFilePath(path.toString());
                DataSet<Tuple2<Integer, String>> source = env.createInput(inputFormat);

                // if (this.parameters.sampleRows > 0) {
                // source = source.filter(new SampleWithHashes<Tuple2<Integer, String>>(this.parameters.sampleRows,
                // 100));
                // }

                // Split the lines into pivot elements.
                SplitMergedCsvRows splitMergedRows;
                // if (this.parameters.isVerifyCsv) {
                // final Int2IntMap numFieldsPerFile = new Int2IntOpenHashMap();
                // for (final Table table : this.schema.getTables()) {
                // numFieldsPerFile.put(table.getId(), table.getColumns().size());
                // }
                // splitRowsFunction = new SplitCsvRows(
                // this.parameters.csvParameters.getFieldSeparatorChar(),
                // this.parameters.csvParameters.getQuoteChar(),
                // numFieldsPerFile, CsvParser.FAIL_ON_ILLEGAL_LINES,
                // this.parameters.maxColumns,
                // !this.parameters.isAllowingEmptyFields);
                // } else {
                splitMergedRows = new SplitMergedCsvRows(
                        csvParameters.getFieldSeparatorChar(),
                        csvParameters.getQuoteChar(),
                        CsvParser.WARN_ON_ILLEGAL_LINES,
                        !isAllowingEmptyFields,
                        idDictionary,
                        metadataStore.getIdUtils()
                );

                final DataSet<Tuple2<Integer, String>> cells = source.flatMap(splitMergedRows);
                cellDataSets.add(cells);
            }

            return PlanBuildingUtils.union(cellDataSets);
        }

        private Map<Path, List<Table>> partitionTablesByFile(Collection<Table> tables) {
            Map<Path, List<Table>> tablesByPath = new HashMap<>();
            for (Table table : tables) {
                MergedCsvFileLocation schemaLocation = (MergedCsvFileLocation) table.getSchema().getLocation();
                Path path = schemaLocation.getPath();
                CollectionUtils.putIntoList(tablesByPath, path, table);
            }
            return tablesByPath;
        }

        /**
         * Creates a translation table pointing table partition IDs within their file into the metadata store minimum
         * column ID.
         */
        private Int2IntMap createIdDictionary(final Collection<Table> tables, IdUtils idUtils) {
            final Int2IntMap idDictionary = new Int2IntOpenHashMap(tables.size());

            for (final Table table : tables) {
                final int schemaNumber = idUtils.getLocalSchemaId(table.getId());
                // Skip empty tables.
                if (table.getColumns().isEmpty()) {
                    continue;
                }
                final int tableNumber = idUtils.getLocalTableId(table.getId());
                final int minColumnId = idUtils.createGlobalId(schemaNumber, tableNumber, idUtils.getMinColumnNumber());
                MergedCsvFilePartitionLocation location = (MergedCsvFilePartitionLocation) table.getLocation();
                idDictionary.put(location.getPartitionId(), minColumnId);
            }
            return idDictionary;
        }

    }

    /**
     * Builds a tuple data set for the given tables.
     *
     * @author Sebastian Kruse
     */
    public static class TupleDataSourceBuilder implements
            DataSourceBuilder<Table, MergedCsvFilePartitionLocation, Tuple> {

        static final TupleDataSourceBuilder INSTANCE = new TupleDataSourceBuilder();

        @Override
        public DataSet<Tuple> buildDataSource(ExecutionEnvironment env,
                                              Collection<Table> allTables,
                                              MetadataStore metadataStore, boolean isAllowingEmptyFields) {

            Map<Path, List<Table>> tablesByCsvParameters = partitionTablesByFile(allTables);
            Collection<DataSet<Tuple>> tupleDataSets = new LinkedList<>();

            for (Map.Entry<Path, List<Table>> partitionEntry : tablesByCsvParameters.entrySet()) {

                // Create a mapping from in-file table IDs to metadata store IDs.
                Path path = partitionEntry.getKey();
                List<Table> tables = partitionEntry.getValue();
                MergedCsvFileLocation schemaLocation = (MergedCsvFileLocation) CollectionUtils.getAny(tables)
                        .getSchema().getLocation();
                CsvParameters csvParameters = schemaLocation.getCsvParameters();
                Object2IntMap<String> pathIds = new Object2IntOpenHashMap<>();
                try {
                    for (Path actualFilePath : FileUtils.gatherFiles(1, path)) {
                        pathIds.put(actualFilePath.toString(), 0);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Could not determine input files.", e);
                }
                final MultiFileTextInputFormat.ListBasedFileIdRetriever fileIdRetriever = new MultiFileTextInputFormat.ListBasedFileIdRetriever(
                        pathIds);
                MultiFileTextInputFormat inputFormat;
                inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever);
                inputFormat.setFilePath(path.toString());
                DataSet<Tuple2<Integer, String>> source = env.createInput(inputFormat);

                Int2IntMap idDictionary = createIdDictionary(tables, metadataStore.getIdUtils());
                DataSet<Tuple> tuples = source
                        .map(new ParseCsvRows(csvParameters.getFieldSeparatorChar(), csvParameters.getQuoteChar()))
                        .map(new EscapeMCSVTuple(idDictionary));

                tupleDataSets.add(tuples);
            }

            return PlanBuildingUtils.union(tupleDataSets);
        }

        private Map<Path, List<Table>> partitionTablesByFile(Collection<Table> tables) {
            Map<Path, List<Table>> tablesByPath = new HashMap<>();
            for (Table table : tables) {
                MergedCsvFileLocation schemaLocation = (MergedCsvFileLocation) table.getSchema().getLocation();
                Path path = schemaLocation.getPath();
                CollectionUtils.putIntoList(tablesByPath, path, table);
            }
            return tablesByPath;
        }

        /**
         * Creates a translation table pointing table partition IDs within their file into the metadata store table ID.
         */
        private Int2IntMap createIdDictionary(final Collection<Table> tables, IdUtils idUtils) {
            final Int2IntMap idDictionary = new Int2IntOpenHashMap(tables.size());

            for (final Table table : tables) {
                // Skip empty tables.
                if (table.getColumns().isEmpty()) {
                    continue;
                }
                MergedCsvFilePartitionLocation location = (MergedCsvFilePartitionLocation) table.getLocation();
                idDictionary.put(location.getPartitionId(), table.getId());
            }
            return idDictionary;
        }

    }

    /**
     * Corrects tuples by exchanging the file table ID with the metadata store table ID.
     *
     * @author Sebastian Kruse
     */
    @FunctionAnnotation.ForwardedFields("tableId")
    public static class EscapeMCSVTuple implements MapFunction<Tuple, Tuple> {

        private final Int2IntMap idDictionary;

        public EscapeMCSVTuple(Int2IntMap idDictionary) {
            super();
            this.idDictionary = idDictionary;
        }

        @Override
        public Tuple map(Tuple tuple) throws Exception {
            int translatedId = this.idDictionary.get(tuple.getMinColumnId());
            tuple.setMinColumnId(translatedId);
            return tuple;
        }

    }

    @Override
    public Collection<String> getAllPropertyKeys() {
        Collection<String> allPropertyKeys = new LinkedList<>(super.getAllPropertyKeys());
        allPropertyKeys.add(PARTITION_ID);
        return allPropertyKeys;
    }
}
