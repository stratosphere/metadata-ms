package de.hpi.isg.mdms.flink.data.kmeans;

import org.apache.flink.api.java.tuple.Tuple1;

import java.util.*;

/**
 * A ucc data point with an id and a set of columns.
 */
public class UCCDataPoint extends DataPoint<Tuple1<int[]>> {

    public Set<Integer> columns = new HashSet<Integer>();

    public HashMap<Integer, Integer> summedColumns = new HashMap<Integer, Integer>();

    public UCCDataPoint(long id) {
        super(id);
    }

    public UCCDataPoint(long id, int[] x) {
        super(id);
        for (int i = 0; i < x.length; i++) {
            this.columns.add(x[i]);
            this.summedColumns.put(x[i], 1);
        }
    }

    public UCCDataPoint(DataPoint<Tuple1<int[]>> d) {
        this(d.id, d.getData().f0);

    }

    public UCCDataPoint add(UCCDataPoint p) {
        for (Map.Entry<Integer, Integer> entry : p.summedColumns.entrySet()) {
            Integer count = this.summedColumns.get(entry.getKey());
            if (count == null) count = 0;
            this.summedColumns.put(entry.getKey(), count + entry.getValue());
        }

        return this;
    }

    public Tuple1<int[]> div(int count) {
        int[] newColumns;
        int indexCounter = 0;
        int totalColumns = 0;

        SortedMap<Integer, List<Integer>> sorted = new TreeMap<>(Collections.reverseOrder());
        for (Map.Entry<Integer, Integer> entry : this.summedColumns.entrySet()) {
            totalColumns = totalColumns + entry.getValue();
            List<Integer> columns = sorted.get(entry.getValue());
            if (columns == null) columns = new ArrayList<>();
            columns.add(entry.getKey());
            sorted.put(entry.getValue(), columns);
        }

        int numberOfNewColumns = Math.round((float) totalColumns / count);
        newColumns = new int[numberOfNewColumns];

        outerloop:
        for (Map.Entry<Integer, List<Integer>> entry : sorted.entrySet()) {
            for (Integer column : entry.getValue()) {
                newColumns[indexCounter] = column;
                indexCounter++;
                if (indexCounter > numberOfNewColumns) break outerloop;
            }
        }

        return new Tuple1<>(newColumns);
    }

    @Override
    public String toString() {
        return "UCC " + id + ": " + this.columns;
    }

}
