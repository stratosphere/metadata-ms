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
package de.hpi.isg.mdms.cassandra.testJobs;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.hadoop.cassandra.CqlFlinkOutputFormat;
import de.hpi.isg.sodap.flink.jobs.AbstractMetadataStoreJob;
import de.hpi.isg.sodap.flink.util.PlanBuildingUtils;
import de.hpi.isg.sodap.util.configuration.MetadataStoreParameters;
import de.hpi.isg.sodap.util.configuration.StratosphereParameters;
import de.hpi.isg.mdms.cassandra.testJobs.CountDistinctValuesCQL.Parameters;

public class CountDistinctValuesCQL extends AbstractMetadataStoreJob<Parameters> {

    private static final Logger LOG = LoggerFactory.getLogger(CountDistinctValuesCQL.class);

    public static void main(final String[] args) throws Exception {
    	
        new CountDistinctValuesCQL(args).run();
    }

    private Parameters parameters;

    private Schema schema;

    private ConstraintCollection constraintCollection;

    public CountDistinctValuesCQL(final String[] args) {
        super(args);
    }

    @Override
    protected Parameters createParameters() {
        return new Parameters();
    }

    @Override
    protected void executeProgramLogic() throws Exception {
        LOG.info("Loading schema.");
        this.schema = getSchema(this.parameters.schemaId, this.parameters.schemaName);

        LOG.info("Planning cell data set.");
        DataSet<Tuple2<Integer, String>> cells = PlanBuildingUtils.buildCellDataSet(this.executionEnvironment, 
        		this.schema.getTables(), 
        		this.metadataStore,
        		true);
        
        // XXX: Little hack: clear the schema cache, so that the loaded tables will be evicted from the cache
        if (schema instanceof RDBMSSchema) {
            ((RDBMSSchema) schema).cacheChildTables(null);
        }
        
        LOG.info("Planning the rest of the job.");
        DataSet<Tuple2<Integer, String>> distinctCells = cells
                .groupBy(0, 1)
                .reduce(new KeepAny<Tuple2<Integer, String>>());

        DataSet<Tuple2<String,ArrayList<Object>>> distinctValueCounts;
            
        distinctValueCounts = distinctCells
                .groupBy(0)
                .reduceGroup(new CountColumnGroup());


        LOG.info("Adding constraint collection.");

        String constraintsDescription = String.format("DVCs for %s (%s)", 
                this.schema.getName(), DateFormat.getInstance().format(new Date()));
        this.constraintCollection = this.metadataStore.createConstraintCollection(constraintsDescription, this.schema);

        
        HadoopOutputFormat<String, ArrayList<Object>> hadoopOutputFormat = 
        	    new HadoopOutputFormat<String,ArrayList<Object>>(
        	      new CqlFlinkOutputFormat(), new JobConf());
    	  hadoopOutputFormat.getJobConf().set("cassandra.output.keyspace", "metadatastore");
    	  hadoopOutputFormat.getJobConf().set("mapreduce.output.basename", "constraintt");
    	  hadoopOutputFormat.getJobConf().set("cassandra.columnfamily.schema.constraintt", 
    			  "CREATE TABLE metadatastore.constraintt ("
			+ "id uuid PRIMARY KEY,"
			+ "type text,"
			+ "constraint_collection_id int,"
			+ "lhs_FD set<int>,"
			+ "rhs_FD int,"
			+ "pair_IND map<int, int>," //lhs_rhs
			+ "UCC set<int>,"
			+ "countt text,"
			+ "count_columnID text);");
    	  hadoopOutputFormat.getJobConf().set("cassandra.columnfamily.insert.constraintt",
    			  "INSERT INTO metadatastore.constraintt (id, type, constraint_collection_id, countt, count_columnID) VALUES (?,'DVC',"+ constraintCollection.getId()+",?,?);");
        	  // OrderPreservingPartitioner as an alternative
        	  hadoopOutputFormat.getJobConf().set("cassandra.output.partitioner.class", "org.apache.cassandra.dht.Murmur3Partitioner");
        	  hadoopOutputFormat.getJobConf().set("cassandra.output.thrift.port","9160");    // default
        	  hadoopOutputFormat.getJobConf().set("cassandra.output.thrift.address", "172.16.18.18"); //localhost
        	  hadoopOutputFormat.getJobConf().set("mapreduce.output.bulkoutputformat.streamthrottlembits", "400");
        	          	  
        	  distinctValueCounts.output(hadoopOutputFormat);

        	  final ExecutorService executorService = Executors.newSingleThreadExecutor();
        
        this.executePlan("Count distinct values");
        RemoteCollectorImpl.shutdownAll();
        
        LOG.debug("Shutting down IND store executor.");
        executorService.shutdown();
        LOG.debug("Awaiting termination of IND store executor.");
        executorService.awaitTermination(365, TimeUnit.DAYS);

        this.metadataStore.flush();
    }
    
    @Override
    protected StratosphereParameters getStratosphereParameters() {
        return this.parameters.stratosphereParameters;
    }

    @Override
    protected void initialize(final String... args) {
        this.parameters = this.parseCommandLine(args);
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    public static final class KeepAny<T> extends RichReduceFunction<T> {

        private static final long serialVersionUID = -1217822558113747023L;

        @Override
        public T reduce(final T value1, final T value2) throws Exception {
        	LOG.info("value: " + value1);
            return value1;
        }
    }

    @SuppressWarnings("serial")
    private static final class CountColumnGroup extends RichGroupReduceFunction<Tuple2<Integer, String>, Tuple2<String,ArrayList<Object>>> {

        @Override
        public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<String,ArrayList<Object>>> out) throws Exception {
            Tuple2<Integer, String> anyInValue = null;
            Integer groupCount = 0;
            for (Tuple2<Integer, String> value : values) {
                anyInValue = value;
                if (anyInValue.f1 != null && !anyInValue.f1.isEmpty()) {
                    groupCount++;
                }
            }
            
            ArrayList<Object> columnsToAdd = new ArrayList<Object>();
            UUID id = getNextID();
            //(id, type, constraint_collection_id, countt, count_columnID)
            columnsToAdd.add(id);
            columnsToAdd.add(groupCount.toString());
            columnsToAdd.add(anyInValue.f0.toString());
            LOG.info("Add value: " + columnsToAdd.get(0) + " " + columnsToAdd.get(1) + " " + columnsToAdd.get(2));
            out.collect(new Tuple2<>("", columnsToAdd));
        }

    }

	private static UUID getNextID() {
		UUID id = UUID.randomUUID();
		return  id;
	}

    /**
     * Parameters for the execution of the surrounding class.
     * 
     * @author Sebastian Kruse
     */
    public static class Parameters implements Serializable {

        private static final long serialVersionUID = 2936720486536771056L;

        @Parameter(names = { MetadataStoreParameters.SCHEMA_ID },
                description = MetadataStoreParameters.SCHEMA_ID_DESCRIPTION, required = false)
        public Integer schemaId;

        @Parameter(names = { MetadataStoreParameters.SCHEMA_NAME },
                description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION, required = false)
        public String schemaName;

        @Parameter(names = { "--use-empty-fields" },
                description = "whether to consider empty fields in the input data",
                required = false)
        public boolean isUseEmptyFields = false;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @ParametersDelegate
        public final StratosphereParameters stratosphereParameters = new StratosphereParameters();

    }
}