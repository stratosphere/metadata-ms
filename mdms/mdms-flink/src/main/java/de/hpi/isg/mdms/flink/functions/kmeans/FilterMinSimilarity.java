package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.kmeans.DataPoint;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Filters and leaves only points which similarity to cluster center is below the threshold,
 */
public final class FilterMinSimilarity<T extends Tuple, D extends DataPoint<T>> implements FilterFunction<Tuple3<Long, D, Double>> {

    double minSimilarity = 0;

    public FilterMinSimilarity(double minSimilarity) {
        this.minSimilarity = minSimilarity;
    }

    @Override
    public boolean filter(Tuple3<Long, D, Double> c) throws Exception {
        return c.f2 < minSimilarity;
    }

}
