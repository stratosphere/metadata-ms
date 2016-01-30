package de.hpi.isg.mdms.flink.data;

/**
 * A centroid of a cluster, basically a datapoint.
 */
public class Centroid<T extends org.apache.flink.api.java.tuple.Tuple, D extends DataPoint<T>> extends DataPoint<T>{

    public Centroid(long id){
        super(id);
    }

    public Centroid(long id, D d) {
        super(id);
    }

    public Centroid(long id, T x) {
        super(id, x);
    }

    public double getSimilarity(D p){
        return 0.0;
    };

    public static Centroid createNewCentroid(long id, DataPoint x) {
        return new Centroid(id, x);
    }

    public static Centroid createNewCentroid(long id, org.apache.flink.api.java.tuple.Tuple x) {
        return new Centroid(id, x);
    }

    @Override
    public String toString() {
        return "ClusterCenter " + id;
    }


}
