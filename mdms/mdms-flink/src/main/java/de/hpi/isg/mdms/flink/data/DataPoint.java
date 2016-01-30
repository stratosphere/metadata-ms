package de.hpi.isg.mdms.flink.data;

/**
 * A data point with an id and a some data.
 */
public class DataPoint<T extends org.apache.flink.api.java.tuple.Tuple> {

    public long id;
    public T tupleData;

    public DataPoint(long id) {
        this.id = id;
    }

    public DataPoint(long id, T x) {
        this.id = id;
        this.tupleData = x;
    }

    public DataPoint add(DataPoint p){
        return this;
    }

    public T div(int count){
        return this.getData();
    }

    public T getData() {
        return this.tupleData;
    }

    public static <T extends org.apache.flink.api.java.tuple.Tuple> DataPoint createNewPoint(long id, T x){
        return new DataPoint<>(id, x);
    }

}
