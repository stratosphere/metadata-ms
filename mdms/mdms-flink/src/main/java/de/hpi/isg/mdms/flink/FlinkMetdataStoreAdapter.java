package de.hpi.isg.mdms.flink;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.tuple.Tuple;

import de.hpi.isg.mdms.flink.serializer.AbstractFlinkSerializer;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;


public class FlinkMetdataStoreAdapter implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public static <T extends Tuple> void save(DataSet<T> constraints, final ConstraintCollection constraintCollection, final AbstractFlinkSerializer flinkSerializer){
		
		final ExecutorService executorService = Executors.newSingleThreadExecutor();
		RemoteCollectorImpl.collectLocal(constraints, new RemoteCollectorConsumer<T>() {
			
			            @Override
			            public void collect(T tuple) {
			                executorService.execute(flinkSerializer.getAddRunnable(tuple, constraintCollection));
			            }
			        });
		}

	public static <T extends Tuple> DataSet<T> getConstraintsFromCollection(
			ExecutionEnvironment executionEnvironment,
			MetadataStore metadataStore,
			ConstraintCollection datasourceCollection, AbstractFlinkSerializer flinkSerializer) {

		return flinkSerializer.getConstraintsFromCollection(executionEnvironment, metadataStore, datasourceCollection);
	}

}
