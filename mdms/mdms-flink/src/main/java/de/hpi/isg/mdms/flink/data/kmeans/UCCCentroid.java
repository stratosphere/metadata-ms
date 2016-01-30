package de.hpi.isg.mdms.flink.data.kmeans;

import org.apache.flink.api.java.tuple.Tuple1;

import java.util.HashSet;
import java.util.Set;

/**
 * A ucc centroid using jaccard similarity.
 */
public class UCCCentroid extends Centroid<Tuple1<int[]>, UCCDataPoint> {

    public Set<Integer> columns = new HashSet<>();

    public UCCCentroid(long id, UCCDataPoint d) {
        super(id, d.getData());
        int[] x = d.getData().f0;
        for (int i = 0; i < x.length; i++) {
            this.columns.add(x[i]);
        }
    }

    public UCCCentroid(long id, int[] x) {
        super(id);
        for (int i = 0; i < x.length; i++) {
            this.columns.add(x[i]);
        }
    }

    public UCCCentroid(long id, Set<Integer> x) {
        super(id);
        this.columns = x;
    }

    public UCCCentroid(Centroid<Tuple1<int[]>, UCCDataPoint> c) {
        this(c.id, c.getData().f0);
    }

    public double getSimilarity(UCCDataPoint p) {

        //jaccard similarity

        Set<Integer> intersection = new HashSet<>(p.columns);
        intersection.retainAll(this.columns);

        Set<Integer> union = new HashSet<>(p.columns);
        union.addAll(this.columns);

        return ((double) intersection.size()) / (union.size());
    }

    @Override
    public String toString() {
        return "ClusterCenter " + id + ": " + this.columns;
    }


}
