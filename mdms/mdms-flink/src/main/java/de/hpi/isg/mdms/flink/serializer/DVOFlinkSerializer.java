package de.hpi.isg.mdms.flink.serializer;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.DistinctValueOverlap;
import de.hpi.isg.mdms.flink.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

public class DVOFlinkSerializer implements AbstractFlinkSerializer<DistinctValueOverlap, Tuple3<Integer, Integer, Integer>> {

	@Override
	public DistinctValueOverlap buildAndAddToCollection(Tuple3<Integer, Integer, Integer> tuple, ConstraintCollection constraintCollection) {

		DistinctValueOverlap constraint;
			synchronized (constraintCollection) {
		              constraint =  DistinctValueOverlap.buildAndAddToCollection(tuple.f0, 
		                        new DistinctValueOverlap.Reference(tuple.f1, tuple.f2), constraintCollection);
		            }
		return constraint;
    }


	@Override
	public DataSet<Tuple3<Integer, Integer, Integer>> getConstraintsFromCollection(
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
		                     .setQuery("select overlap, column1, column2 from DistinctValueOverlap "
		                     		+ "where constraintId in "
		                     		+ "(select id from Constraintt where constraintCollectionId = '" + datasourceCollection.getId() + "')")
		                     .finish(),
		      // specify type information for DataSet
		      new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
		    );

		
		return dbData;

	}

}
