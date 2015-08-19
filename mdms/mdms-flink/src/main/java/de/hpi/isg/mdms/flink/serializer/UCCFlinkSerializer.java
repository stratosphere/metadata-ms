package de.hpi.isg.mdms.flink.serializer;


import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.flink.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

public class UCCFlinkSerializer implements AbstractFlinkSerializer<UniqueColumnCombination, Tuple1<int[]>> {

	private class AddUCCCommand implements Runnable {
		
        private int[] columnIds;
        private int numDistinctValues;
        private ConstraintCollection constraintCollection;

        public AddUCCCommand(final Tuple1<int[]> tuple, final ConstraintCollection constraintCollection) {
            this.columnIds = tuple.f0;
            this.constraintCollection = constraintCollection;
        }

        @Override
        public void run() {
            UniqueColumnCombination constraint;
                    synchronized (this.constraintCollection) {
                    	final UniqueColumnCombination.Reference reference = new UniqueColumnCombination.Reference(
                                columnIds);
                        constraint = UniqueColumnCombination.buildAndAddToCollection(reference,
                                this.constraintCollection);
                    }
        }
    }

	
	
	@Override
	public DataSet<Tuple1<int[]>> getConstraintsFromCollection(
			ExecutionEnvironment executionEnvironment,
			MetadataStore metadataStore,
			ConstraintCollection datasourceCollection) {
		
		// Read data from a relational database using the JDBC input format
		RDBMSMetadataStore rdbms = (RDBMSMetadataStore) metadataStore;
		
		DataSet<Tuple2<Integer, Integer>> dbData = 
		    executionEnvironment.createInput(
		      // create and configure input format
		      SqLiteJDBCInputFormat.buildJDBCInputFormat()
		                     .setDrivername("org.sqlite.JDBC")
		                     .setDBUrl(rdbms.getSQLInterface().getDatabaseURL())
		                     .setQuery("select constraintId, col from UCCPart "
		                     		+ "where constraintId in "
		                     		+ "(select id from Constraintt where constraintCollectionId = '" + datasourceCollection.getId() + "')")
		                     .finish(),
		      // specify type information for DataSet
		      new TupleTypeInfo<Tuple2<Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
		    );

		DataSet<Tuple1<int[]>> uccs = dbData.groupBy(0)
										.reduceGroup(new CreateUCCs());
		
		return uccs;

	}
	
    @SuppressWarnings("serial")
    private static final class CreateUCCs implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple1<int[]>> {

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> values,
				Collector<Tuple1<int[]>> out) throws Exception {

			ArrayList<Integer> columns = new ArrayList<Integer>();
			for (Tuple2<Integer, Integer> tuple: values) {
				columns.add(tuple.f1);
			}
			out.collect(new Tuple1<int[]>(ArrayUtils.toPrimitive(columns.toArray(new Integer[0]))));
		}
        
    }

	@Override
	public Runnable getAddRunnable(Tuple1<int[]> tuple,
			ConstraintCollection constraintCollection) {
		return new AddUCCCommand(tuple, constraintCollection);
	}    
    
}



