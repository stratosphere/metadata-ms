package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.Centroid;
import de.hpi.isg.mdms.flink.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Determines the closest cluster center for a data point.
 */
public final class SelectNearestCenterReturnSimilarity<T extends Tuple, D extends DataPoint<T>, C extends Centroid<T, D>> extends RichMapFunction<D, Tuple3<Long, D, Double>> {
    private Collection<C> centroids;

    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple3<Long, D, Double> map(D p) throws Exception {

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
        return new Tuple3<>(closestCentroidId, p, maxSimilarity);
    }
}
