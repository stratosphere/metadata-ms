package de.hpi.isg.mdms.flink.serializer;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;


public interface AbstractFlinkSerializer<T extends Constraint, F extends Tuple> {
	
    public abstract DataSet<F> getConstraintsFromCollection(
			ExecutionEnvironment executionEnvironment,
			MetadataStore metadataStore,
			ConstraintCollection datasourceCollection);
    
	public abstract Runnable getAddRunnable(F tuple, ConstraintCollection constraintCollection);
	
}
