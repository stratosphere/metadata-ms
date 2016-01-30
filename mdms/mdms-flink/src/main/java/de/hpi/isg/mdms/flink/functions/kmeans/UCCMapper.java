package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.DataPoint;
import de.hpi.isg.mdms.flink.data.UCCDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Maps the datapoints to uccDatapoints.
 */
public final class UCCMapper<D extends DataPoint<Tuple1<int[]>>> extends RichMapFunction<Tuple3<Long, D, Double>, Tuple3<Long, UCCDataPoint, Double>> {

    @Override
    public Tuple3<Long, UCCDataPoint, Double> map(Tuple3<Long, D, Double> c) throws Exception {

        return new Tuple3<>(c.f0, new UCCDataPoint(c.f1), c.f2);
    }
}
