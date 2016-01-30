package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.Centroid;
import de.hpi.isg.mdms.flink.data.DataPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

import java.util.List;

public final class PointCentroidConverter<T extends Tuple, D extends DataPoint<T>, C extends Centroid<T, D>> implements FlatMapFunction<D, Centroid<T, D>> {

    private List<Long> keepIDs;

    public void setKeepIDs(List<Long> ids) {
        keepIDs = ids;
    }

    public void flatMap(D t, Collector<Centroid<T, D>> out) {
        if (keepIDs.contains(t.id)) {
            out.collect(Centroid.createNewCentroid(t.id, t.getData()));
        }
    }
}
