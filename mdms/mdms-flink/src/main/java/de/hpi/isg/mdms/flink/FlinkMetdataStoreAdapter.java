package de.hpi.isg.mdms.flink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;

import de.hpi.isg.mdms.flink.serializer.AbstractFlinkSerializer;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;


public class FlinkMetdataStoreAdapter implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public static <T extends Tuple> void save(DataSet<T> constraints, ConstraintCollection constraintCollection, AbstractFlinkSerializer flinkSerializer){
		List<Tuple> constraintsList = new ArrayList<Tuple>();
		constraints.output(new LocalCollectionOutputFormat(constraintsList));
		for (Tuple tuple: constraintsList){
			flinkSerializer.buildAndAddToCollection(tuple, constraintCollection);
		}
	}

	public static <T extends Tuple> DataSet<T> getConstraintsFromCollection(
			ExecutionEnvironment executionEnvironment,
			MetadataStore metadataStore,
			ConstraintCollection datasourceCollection, AbstractFlinkSerializer flinkSerializer) {

		return flinkSerializer.getConstraintsFromCollection(executionEnvironment, metadataStore, datasourceCollection);
	}

}
