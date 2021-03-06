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

import de.hpi.isg.mdms.Encoding;
import de.hpi.isg.mdms.clients.location.AbstractCsvLocation;
import de.hpi.isg.mdms.clients.location.CsvFileLocation;
import de.hpi.isg.mdms.clients.location.MergedCsvFileLocation;
import de.hpi.isg.mdms.clients.parameters.CsvParameters;
import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.flink.functions.FilterIncompleteTuples;
import de.hpi.isg.mdms.flink.functions.ParseCsvRows;
import de.hpi.isg.mdms.flink.functions.SplitCsvRows;
import de.hpi.isg.mdms.flink.readwrite.MultiFileTextInputFormat;
import de.hpi.isg.mdms.flink.util.FileUtils;
import de.hpi.isg.mdms.flink.util.PlanBuildingUtils;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.util.CollectionUtils;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

/**
 * Utilities for handling {@link de.hpi.isg.mdms.clients.location.AbstractCsvLocation} with Flink.
 *
 * @author Sebastian Kruse
 */
public class CsvDataSourceBuilders {

    public static <T extends AbstractCsvLocation> DataSourceBuilder<Tuple2<Integer, String>> getCellDataSourceBuilder(T location) {
        if (location instanceof CsvFileLocation) {
            return CsvCellDataSourceBuilder.INSTANCE;
        } else if (location instanceof MergedCsvFileLocation) {
            return MergedCsvCellDataSourceBuilder.INSTANCE;
        } else {
            throw new IllegalArgumentException("Cannot provide cell data source builder for given location.");
        }
    }

    public static <T extends AbstractCsvLocation> DataSourceBuilder<Tuple> getTupleDataSourceBuilder(T location) {
        if (location instanceof CsvFileLocation) {
            return CsvTupleDataSourceBuilder.INSTANCE;
        } else if (location instanceof MergedCsvFileLocation) {
            return MergedCsvTupleDataSourceBuilder.INSTANCE;
        } else {
            throw new IllegalArgumentException("Cannot provide tuple data source builder for given location.");
        }
    }

    public static class CsvCellDataSourceBuilder implements DataSourceBuilder<Tuple2<Integer, String>> {

        static final CsvCellDataSourceBuilder INSTANCE = new CsvCellDataSourceBuilder();

        @Override
        public DataSet<Tuple2<Integer, String>> buildDataSource(ExecutionEnvironment env, Collection<? extends Target> allTables,
                                                                MetadataStore metadataStore, boolean isAllowingEmptyFields) {

            Map<Tuple2<CsvParameters, Encoding>, List<Table>> tablesByCsvParameters = partitionTablesByParameters(allTables);

            Collection<DataSet<Tuple2<Integer, String>>> cellDataSets = new LinkedList<>();

            for (Map.Entry<Tuple2<CsvParameters, Encoding>, List<Table>> partitionEntry : tablesByCsvParameters.entrySet()) {
                CsvParameters csvParameters = partitionEntry.getKey().f0;
                Encoding encoding = partitionEntry.getKey().f1;

                List<Table> tables = partitionEntry.getValue();
                // Check if any of the files has a compression suffix.
                DataSet<Tuple2<Integer, String>> source;

                Object2IntMap<String> pathIds = collectPathIds(tables, metadataStore.getIdUtils());
                String inputPath = FileUtils.findCommonParent(pathIds.keySet());
                final MultiFileTextInputFormat.ListBasedFileIdRetriever fileIdRetriever = new MultiFileTextInputFormat.ListBasedFileIdRetriever(pathIds);
                MultiFileTextInputFormat inputFormat;
                inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever, null);
                inputFormat.setEncoding(encoding);
                // TODO: Enable if needed.
                inputFormat.setFilePath(inputPath);
//                inputFormat.setRecordDetector(new CsvRecordStateMachine(csvParameters.getFieldSeparatorChar(),
//                        csvParameters.getQuoteChar(), '\n', encoding.getCharset()));
                source = env.createInput(inputFormat).name(String.format("CSV files (%d tables)", tables.size()));

                // if (this.parameters.sampleRows > 0) {
                // source = source.filter(new SampleWithHashes<Tuple2<Integer, String>>(this.parameters.sampleRows,
                // 100));
                // }

                // Split the lines into pivot elements.
                SplitCsvRows splitRowsFunction;
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
                splitRowsFunction = new SplitCsvRows(
                        csvParameters.getFieldSeparatorChar(),
                        csvParameters.getQuoteChar(),
                        null,
                        -1,
                        -1, // this.parameters.maxColumns,
                        csvParameters.getNullString(),
                        !isAllowingEmptyFields // !this.parameters.isAllowingEmptyFields
                );
                // }

                final DataSet<Tuple2<Integer, String>> cells = source.flatMap(splitRowsFunction).name("Split rows");
                cellDataSets.add(cells);
            }

            return PlanBuildingUtils.union(cellDataSets, "Union cells");
        }

    }

    public static class CsvTupleDataSourceBuilder implements DataSourceBuilder<Tuple> {

        static final CsvTupleDataSourceBuilder INSTANCE = new CsvTupleDataSourceBuilder();

        @Override
        public DataSet<Tuple> buildDataSource(ExecutionEnvironment env,
                                              Collection<? extends Target> allTables,
                                              MetadataStore metadataStore,
                                              boolean isAllowingEmptyFields) {

            // Allocate a list to collect the data sinks.
            Collection<DataSet<Tuple>> tupleDataSets = new LinkedList<>();

            Map<Tuple2<CsvParameters, Encoding>, List<Table>> tablesByCsvParameters = partitionTablesByParameters(allTables);

            for (Map.Entry<Tuple2<CsvParameters, Encoding>, List<Table>> partitionEntry : tablesByCsvParameters.entrySet()) {
                CsvParameters csvParameters = partitionEntry.getKey().f0;
                Encoding encoding = partitionEntry.getKey().f1;

                List<Table> tables = partitionEntry.getValue();
                // Check if any of the files has a compression suffix.
                DataSet<Tuple2<Integer, String>> source;

                Object2IntMap<String> pathIds = collectPathIds(tables, metadataStore.getIdUtils());
                String inputPath = FileUtils.findCommonParent(pathIds.keySet());
                final MultiFileTextInputFormat.ListBasedFileIdRetriever fileIdRetriever =
                        new MultiFileTextInputFormat.ListBasedFileIdRetriever(pathIds);
                MultiFileTextInputFormat inputFormat;
                inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever, null);
                inputFormat.setEncoding(encoding);
                inputFormat.setFilePath(inputPath);
                // TODO: Enable as needed.
                // inputFormat.setRecordDetector(new CsvRecordStateMachine(csvParameters.getFieldSeparatorChar(),
//                        csvParameters.getQuoteChar(), '\n', encoding.getCharset()));
                source = env.createInput(inputFormat).name(String.format("CSV files (%d tables)", tables.size()));

                // if (this.parameters.sampleRows > 0) {
                // source = source.filter(new SampleWithHashes<Tuple2<Integer, String>>(this.parameters.sampleRows,
                // 100));
                // }

                // Split the lines into pivot elements.
                ParseCsvRows parseRows;
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
                parseRows = new ParseCsvRows(
                        csvParameters.getFieldSeparatorChar(),
                        csvParameters.getQuoteChar(),
                        csvParameters.getNullString()
                );
                // }

                final DataSet<Tuple> tuples = source.map(parseRows).name("Parse rows");
                tupleDataSets.add(tuples);
            }

            DataSet<Tuple> tuples = PlanBuildingUtils.union(tupleDataSets, "Union tables");
            if (!isAllowingEmptyFields) {
                tuples = tuples.filter(new FilterIncompleteTuples()).name("Filter incomplete tuples");
            }

            return tuples;
        }
    }

    public static class MergedCsvCellDataSourceBuilder implements DataSourceBuilder<Tuple2<Integer, String>> {

        public static final MergedCsvCellDataSourceBuilder INSTANCE = new MergedCsvCellDataSourceBuilder();

        @Override
        public DataSet<Tuple2<Integer, String>> buildDataSource(ExecutionEnvironment env, Collection<? extends Target> schemata,
                                                                MetadataStore metadataStore, boolean isAllowingEmptyFields) {

            Collection<DataSet<Tuple2<Integer, String>>> schemaDataSets = new LinkedList<>();
            for (Target target : schemata) {
                if (!(target instanceof Schema)) {
                    throw new IllegalArgumentException("Expected merged CSV file to represent a schema.");
                }
                Schema schema = (Schema) target;
                schemaDataSets.add(PlanBuildingUtils.buildCellDataSet(env, schema.getTables(), metadataStore, isAllowingEmptyFields));
            }

            return PlanBuildingUtils.union(schemaDataSets);
        }

    }

    public static class MergedCsvTupleDataSourceBuilder implements DataSourceBuilder<Tuple> {

        public static final MergedCsvTupleDataSourceBuilder INSTANCE = new MergedCsvTupleDataSourceBuilder();

        @Override
        public DataSet<Tuple> buildDataSource(ExecutionEnvironment env, Collection<? extends Target> schemata,
                                              MetadataStore metadataStore, boolean isAllowingEmptyFields) {

            Collection<DataSet<Tuple>> schemaDataSets = new LinkedList<>();
            for (Target target : schemata) {
                if (!(target instanceof Schema)) {
                    throw new IllegalArgumentException("Expected merged CSV file to represent a schema.");
                }
                Schema schema = (Schema) target;
                schemaDataSets.add(PlanBuildingUtils.buildTupleDataSet(env, schema.getTables(), metadataStore));
            }

            return PlanBuildingUtils.union(schemaDataSets);
        }

    }

    /**
     * Creates a mapping from paths to non-standard encodings.
     *
     * @param tables are the tables whose encodings are to collect
     * @return a mapping pointing the tables' paths to their encoding (if it is not the default encoding)
     */
    private static Map<String, Encoding> collectEncodings(List<Table> tables) {
        return AbstractCsvLocation.collectEncodings(tables);
    }


    private static Map<Tuple2<CsvParameters, Encoding>, List<Table>> partitionTablesByParameters(Collection<? extends Target> tables) {
        Map<Tuple2<CsvParameters, Encoding>, List<Table>> tablesByCsvParameters = new HashMap<>();
        for (Target target : tables) {
            if (!(target instanceof Table)) {
                throw new IllegalArgumentException("Expected table.");
            }
            Table table = (Table) target;
            CsvParameters csvParameters = ((AbstractCsvLocation) table.getLocation()).getCsvParameters();
            Encoding encoding = ((AbstractCsvLocation) table.getLocation()).getEncoding();
            CollectionUtils.putIntoList(tablesByCsvParameters, new Tuple2<>(csvParameters, encoding), table);
        }
        return tablesByCsvParameters;
    }


    /**
     * Collect minimum {@link Column#getId()} for each {@link Table} in the {@link Schema}.
     *
     * @return a map that assigns to the path of each table the minimum column ID
     */
    private static Object2IntMap<String> collectPathIds(final Collection<Table> tables, IdUtils idUtils) {
        final Object2IntMap<String> pathIds = new Object2IntOpenHashMap<>();

        for (final Table table : tables) {
            final int schemaNumber = idUtils.getLocalSchemaId(table.getId());
            // Skip empty tables.
            if (table.getColumns().isEmpty()) {
                continue;
            }
            final int tableNumber = idUtils.getLocalTableId(table.getId());
            final int minColumnId = idUtils.createGlobalId(schemaNumber, tableNumber, idUtils.getMinColumnNumber());
            AbstractCsvLocation location = (AbstractCsvLocation) table.getLocation();
            pathIds.put(location.getPath(), minColumnId);
        }
        return pathIds;
    }

}
