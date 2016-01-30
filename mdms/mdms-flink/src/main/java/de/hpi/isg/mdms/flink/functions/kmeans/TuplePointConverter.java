package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.kmeans.DataPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public final class TuplePointConverter<T extends org.apache.flink.api.java.tuple.Tuple>
        implements MapFunction<Tuple2<Long, T>, DataPoint<T>> {

    @Override
    public DataPoint<T> map(Tuple2<Long, T> t) throws Exception {
        return DataPoint.createNewPoint(t.f0, t.f1);
    }
}
