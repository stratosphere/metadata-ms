/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.flink.location;

import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.flink.util.PlanBuildingUtils;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Describes the location of a CSV file.
 * 
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class MergedCsvFileLocation extends AbstractCsvLocation implements CellLocation, TupleLocation {

	@SuppressWarnings("unchecked")
	@Override
	public DataSourceBuilder<Schema, MergedCsvFileLocation, Tuple2<Integer, String>> getCellDataSourceBuilder() {
	    return MergedCsvFileLocation.CellDataSourceBuilder.INSTANCE;
	}


	@SuppressWarnings("unchecked")
    @Override
	public DataSourceBuilder<Schema, MergedCsvFileLocation, Tuple> getTupleDataSourceBuilder() {
	    return MergedCsvFileLocation.TupleDataSourceBuilder.INSTANCE;
	}

    public static class CellDataSourceBuilder implements DataSourceBuilder<Schema, MergedCsvFileLocation, Tuple2<Integer, String>> {

        public static final MergedCsvFileLocation.CellDataSourceBuilder INSTANCE = new MergedCsvFileLocation.CellDataSourceBuilder();

        @Override
        public DataSet<Tuple2<Integer, String>> buildDataSource(ExecutionEnvironment env, Collection<Schema> schemata,
                                                                MetadataStore metadataStore, boolean isAllowingEmptyFields) {

            Collection<DataSet<Tuple2<Integer, String>>> schemaDataSets = new LinkedList<>();
            for (Schema schema : schemata) {
                schemaDataSets.add(PlanBuildingUtils.buildCellDataSet(env, schema.getTables(), metadataStore, isAllowingEmptyFields));
            }

            return PlanBuildingUtils.union(schemaDataSets);
        }

    }

    public static class TupleDataSourceBuilder implements DataSourceBuilder<Schema, MergedCsvFileLocation, Tuple> {

        public static final MergedCsvFileLocation.TupleDataSourceBuilder INSTANCE = new MergedCsvFileLocation.TupleDataSourceBuilder();
        
        @Override
        public DataSet<Tuple> buildDataSource(ExecutionEnvironment env, Collection<Schema> schemata,
                                              MetadataStore metadataStore, boolean isAllowingEmptyFields) {
            
            Collection<DataSet<Tuple>> schemaDataSets = new LinkedList<>();
            for (Schema schema : schemata) {
                schemaDataSets.add(PlanBuildingUtils.buildTupleDataSet(env, schema.getTables(), metadataStore));
            }
            
            return PlanBuildingUtils.union(schemaDataSets);
        }
        
    }




}
