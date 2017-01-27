package de.hpi.isg.mdms.flink.serializer;


import de.hpi.isg.mdms.model.constraints.Constraint;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.DistinctValueCount;
import de.hpi.isg.mdms.domain.constraints.SingleTargetReference;
import de.hpi.isg.mdms.flink.readwrite.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

public class DVCFlinkSerializer implements AbstractFlinkSerializer<DistinctValueCount, Tuple2<Integer, Integer>> {

	private class AddDistinctValueCountCommand implements Runnable {
		
		        private int targetId;
		        private int numDistinctValues;
		        private ConstraintCollection<DistinctValueCount> constraintCollection;
		
		        public AddDistinctValueCountCommand(final Tuple2<Integer, Integer> tuple, final ConstraintCollection<DistinctValueCount> constraintCollection) {
		            this.targetId = tuple.f0;
		            this.numDistinctValues = tuple.f1;
		            this.constraintCollection = constraintCollection;
		        }
		
		        @Override
		        public void run() {
		            DistinctValueCount constraint;
		                    synchronized (this.constraintCollection) {
		                        constraint = DistinctValueCount.buildAndAddToCollection(new SingleTargetReference(this.targetId),
		                                this.constraintCollection, this.numDistinctValues);
		                    }
		        }
		    }

	@Override
	public DataSet<Tuple2<Integer, Integer>> getConstraintsFromCollection(
			ExecutionEnvironment executionEnvironment,
			MetadataStore metadataStore,
			ConstraintCollection<? extends Constraint> datasourceCollection) {
		
		// Read data from a relational database using the JDBC input format
		RDBMSMetadataStore rdbms = (RDBMSMetadataStore) metadataStore;
		
		DataSet<Tuple2<Integer, Integer>> dbData = 
		    executionEnvironment.createInput(
		      // create and configure input format
		      SqLiteJDBCInputFormat.buildJDBCInputFormat()
		                     .setDrivername("org.sqlite.JDBC")
		                     .setDBUrl(rdbms.getSQLInterface().getDatabaseURL())
		                     .setQuery("select columnId, distinctValueCount from DistinctValueCount "
		                     		+ "where  constraintCollectionId = " + datasourceCollection.getId() + "")
		                     .finish(),
		      // specify type information for DataSet
		      new TupleTypeInfo<Tuple2<Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
		    );

		
		return dbData;
	}
	
	@Override
	public Runnable getAddRunnable(Tuple2<Integer, Integer> tuple,
			ConstraintCollection<DistinctValueCount> constraintCollection) {
		return new AddDistinctValueCountCommand(tuple, constraintCollection);
	}

}
