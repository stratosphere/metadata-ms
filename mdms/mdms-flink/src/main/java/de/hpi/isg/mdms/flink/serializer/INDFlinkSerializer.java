package de.hpi.isg.mdms.flink.serializer;


import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.flink.readwrite.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

public class INDFlinkSerializer implements AbstractFlinkSerializer<InclusionDependency, Tuple2<int[], int[]>> {

    private class AddInclusionDependencyCommand implements Runnable {

        private int[] dependent;
		private int[] referenced;
		private ConstraintCollection constraintCollection;

		public AddInclusionDependencyCommand(final Tuple2<int[], int[]> indTuple, final ConstraintCollection constraintCollection) {
            super();
            this.dependent = indTuple.f0;
            this.referenced = indTuple.f1;
            this.constraintCollection = constraintCollection;
        }

        @Override
        public void run() {
        	synchronized(this.constraintCollection){
        		final InclusionDependency.Reference reference = new InclusionDependency.Reference(
                        dependent,
                        referenced);
                InclusionDependency.buildAndAddToCollection(reference, this.constraintCollection);
            }
        }
    }
	
    
    @SuppressWarnings("serial")
        private static final class CreateINDs implements GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<int[], int[]>> {
    
    		@Override
    		public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> values,
    				Collector<Tuple2<int[], int[]>> out) throws Exception {
    
    			ArrayList<Integer> dependent = new ArrayList<Integer>();
    		ArrayList<Integer> referenced = new ArrayList<Integer>();
    			for (Tuple3<Integer, Integer, Integer> tuple: values) {
    				dependent.add(tuple.f1);
    				referenced.add(tuple.f2);
    			}
    			out.collect(new Tuple2<int[], int[]>(ArrayUtils.toPrimitive(dependent.toArray(new Integer[0])),
    					ArrayUtils.toPrimitive(referenced.toArray(new Integer[0]))));
    		}
            
        }
	
	@Override
	public DataSet<Tuple2<int[], int[]>> getConstraintsFromCollection(
			ExecutionEnvironment executionEnvironment,
			MetadataStore metadataStore,
			ConstraintCollection datasourceCollection) {
		
		// Read data from a relational database using the JDBC input format
		RDBMSMetadataStore rdbms = (RDBMSMetadataStore) metadataStore;
		
		DataSet<Tuple3<Integer, Integer, Integer>> dbData = 
		    executionEnvironment.createInput(
		      // create and configure input format
		      SqLiteJDBCInputFormat.buildJDBCInputFormat()
		                     .setDrivername("org.sqlite.JDBC")
		                     .setDBUrl(rdbms.getSQLInterface().getDatabaseURL())
		                     .setQuery("select IND.constraintId, lhs, rhs from IND, INDPart "
		                     		+ "where IND.constraintId = INDPart.constraintId " +
									 "and constraintCollectionId = " + datasourceCollection.getId() + ";")
		                     .finish(),
		      // specify type information for DataSet
		      new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
		    );

		DataSet<Tuple2<int[], int[]>> inds = dbData.groupBy(0)
										.reduceGroup(new CreateINDs());
		
		return inds;

	}

	@Override
	public Runnable getAddRunnable(Tuple2<int[], int[]> tuple,
			ConstraintCollection constraintCollection) {
		return new AddInclusionDependencyCommand(tuple, constraintCollection);
	}
	
	
	
    
}



