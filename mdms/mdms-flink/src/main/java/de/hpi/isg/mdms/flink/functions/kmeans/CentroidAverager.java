package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.kmeans.Centroid;
import de.hpi.isg.mdms.flink.data.kmeans.DataPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Computes new centroid from coordinate sum and count of points.
 */
public final class CentroidAverager<T extends Tuple, D extends DataPoint<T>, C extends Centroid> implements MapFunction<Tuple3<Long, D, Integer>, Centroid<T, D>> {

    @Override
    public Centroid<T, D> map(Tuple3<Long, D, Integer> value) {
        return C.createNewCentroid(value.f0, value.f1.div(value.f2));
    }
}
