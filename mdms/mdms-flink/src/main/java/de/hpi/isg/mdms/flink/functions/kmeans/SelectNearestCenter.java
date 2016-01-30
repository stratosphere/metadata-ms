package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.kmeans.Centroid;
import de.hpi.isg.mdms.flink.data.kmeans.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Determines the closest cluster center for a data point.
 */
public final class SelectNearestCenter<D extends DataPoint, C extends Centroid> extends RichMapFunction<D, Tuple2<Long, D>> {
    private Collection<C> centroids;

    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple2<Long, D> map(D p) throws Exception {

        double maxSimilarity = -1;
        long closestCentroidId = -1;

        // check all cluster centers
        for (C centroid : centroids) {
            // compute distance
            double similarity = centroid.getSimilarity(p);

            // update nearest cluster if necessary
            if (similarity > maxSimilarity) {
                maxSimilarity = similarity;
                closestCentroidId = centroid.id;
            }
        }

        // emit a new record with the center id and the data point.
        return new Tuple2<Long, D>(closestCentroidId, p);
    }
}
