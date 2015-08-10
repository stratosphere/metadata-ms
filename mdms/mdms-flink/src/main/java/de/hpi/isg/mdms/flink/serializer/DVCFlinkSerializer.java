package de.hpi.isg.mdms.flink.serializer;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.DistinctValueCount;
import de.hpi.isg.mdms.domain.constraints.SingleTargetReference;
import de.hpi.isg.mdms.flink.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

public class DVCFlinkSerializer implements AbstractFlinkSerializer<DistinctValueCount, Tuple2<Integer, Integer>> {

	@Override
	public DistinctValueCount buildAndAddToCollection(Tuple2<Integer, Integer> tuple, ConstraintCollection constraintCollection) {

		DistinctValueCount constraint;
			synchronized (constraintCollection) {
		        constraint = DistinctValueCount.buildAndAddToCollection(new SingleTargetReference(tuple.f0),
		                constraintCollection, tuple.f1);
		    }
			return constraint;
    }


	@Override
	public DataSet<Tuple2<Integer, Integer>> getConstraintsFromCollection(
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
		                     .setQuery("select columnId, distinctValueCount from DistinctValueCount "
		                     		+ "where constraintId in "
		                     		+ "(select id from Constraintt where constraintCollectionId = '" + datasourceCollection.getId() + "')")
		                     .finish(),
		      // specify type information for DataSet
		      new TupleTypeInfo<Tuple2<Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
		    );

		
		return dbData;

	}

}
