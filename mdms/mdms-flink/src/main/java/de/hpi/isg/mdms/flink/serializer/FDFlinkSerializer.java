package de.hpi.isg.mdms.flink.serializer;


import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.flink.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

public class FDFlinkSerializer implements AbstractFlinkSerializer<FunctionalDependency, Tuple2<Integer, int[]>> {

	@Override
	public FunctionalDependency buildAndAddToCollection(Tuple2<Integer, int[]> tuple, ConstraintCollection constraintCollection) {

		throw new NotImplementedException();
    }


	@Override
	public DataSet<Tuple2<Integer, int[]>> getConstraintsFromCollection(
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
		                     .setQuery("select FD.constraintId, rhs_col, lhs_col from FD, FD_LHS "
		                     		+ "where FD.constraintId = FD_LHS.constraintId and FD.constraintId in "
		                     		+ "(select id from Constraintt where constraintCollectionId = '" + datasourceCollection.getId() + "')")
		                     .finish(),
		      // specify type information for DataSet
		      new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
		    );

		DataSet<Tuple2<Integer, int[]>> inds = dbData.groupBy(0)
										.reduceGroup(new CreateFDs());
		
		return inds;

	}
	
    @SuppressWarnings("serial")
    private static final class CreateFDs implements GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>> {

		@Override
		public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> values,
				Collector<Tuple2<Integer, int[]>> out) throws Exception {

			Integer rhs = null;
			ArrayList<Integer> lhs = new ArrayList<Integer>();
			for (Tuple3<Integer, Integer, Integer> tuple: values) {
				rhs = tuple.f1;
				lhs.add(tuple.f2);
			}
			out.collect(new Tuple2<Integer, int[]>(rhs,
					ArrayUtils.toPrimitive(lhs.toArray(new Integer[0]))));
		}
        
    }    
    
}



