package de.hpi.isg.mdms.flink.readwrite;

import de.hpi.isg.mdms.flink.serializer.AbstractFlinkSerializer;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.tuple.Tuple;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class FlinkMetdataStoreAdapter implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * @deprecated Don't use this method unless thread leaks are acceptable (e.g., in short-lived apps).
     */
    public static <T extends Tuple> void saveAsync(DataSet<T> constraints, final ConstraintCollection constraintCollection, final AbstractFlinkSerializer flinkSerializer) {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        RemoteCollectorImpl.collectLocal(constraints, tuple -> {
            executorService.execute(flinkSerializer.getAddRunnable(tuple, constraintCollection));
        });
    }

    /**
     * @deprecated Don't use this method unless thread leaks are acceptable (e.g., in short-lived apps).
     */
    public static <T extends Tuple> void save(DataSet<T> constraints, final ConstraintCollection constraintCollection, final AbstractFlinkSerializer flinkSerializer) {

        RemoteCollectorImpl.collectLocal(constraints, tuple -> {
            synchronized (flinkSerializer) {
                flinkSerializer.getAddRunnable(tuple, constraintCollection).run();;
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
