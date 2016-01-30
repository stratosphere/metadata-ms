package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.DataPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Appends a count variable to the tuple.
 */
public final class CountAppender<D extends DataPoint> implements MapFunction<Tuple2<Long, D>, Tuple3<Long, D, Integer>> {

    @Override
    public Tuple3<Long, D, Integer> map(Tuple2<Long, D> t) {
        return new Tuple3<Long, D, Integer>(t.f0, t.f1, 1);
    }
}
