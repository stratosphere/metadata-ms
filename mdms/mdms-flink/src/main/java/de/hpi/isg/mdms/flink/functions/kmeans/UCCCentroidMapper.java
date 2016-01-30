package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.kmeans.Centroid;
import de.hpi.isg.mdms.flink.data.kmeans.UCCCentroid;
import de.hpi.isg.mdms.flink.data.kmeans.UCCDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Maps the centroids to uccCentroids.
 */
public final class UCCCentroidMapper<C extends Centroid<Tuple1<int[]>, UCCDataPoint>>
        extends RichMapFunction<C, UCCCentroid> {

    @Override
    public UCCCentroid map(C c) throws Exception {

        return new UCCCentroid(c);
    }
}
