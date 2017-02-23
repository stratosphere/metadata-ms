package de.hpi.isg.mdms.flink.serializer;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;


public interface AbstractFlinkSerializer<T, F extends Tuple> {

    DataSet<F> getConstraintsFromCollection(
            ExecutionEnvironment executionEnvironment,
            MetadataStore metadataStore,
            ConstraintCollection<?> datasourceCollection);

    Runnable getAddRunnable(F tuple, ConstraintCollection<T> constraintCollection);
}
