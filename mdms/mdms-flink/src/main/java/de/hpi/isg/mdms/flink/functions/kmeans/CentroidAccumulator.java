package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.kmeans.DataPoint;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Sums and counts point coordinates.
 */
public final class CentroidAccumulator<D extends DataPoint> implements ReduceFunction<Tuple3<Long, D, Integer>> {

    @Override
    public Tuple3<Long, D, Integer> reduce(Tuple3<Long, D, Integer> val1, Tuple3<Long, D, Integer> val2) {
        return new Tuple3<>(val1.f0, (D) val1.f1.add(val2.f1), val1.f2 + val2.f2);
    }
}
