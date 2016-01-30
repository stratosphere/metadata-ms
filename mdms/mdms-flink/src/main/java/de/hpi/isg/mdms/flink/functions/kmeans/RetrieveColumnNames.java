package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.UCCDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Maps column ids to column names.
 */
public final class RetrieveColumnNames extends RichMapFunction<Tuple3<Long, UCCDataPoint, Double>, Tuple3<Long, HashSet<String>, Double>> {
    private HashMap<Integer, String> columnNames;

    public void setColumnNames(HashMap<Integer, String> map) {
        columnNames = map;
    }


    @Override
    public Tuple3<Long, HashSet<String>, Double> map(Tuple3<Long, UCCDataPoint, Double> t) throws Exception {

        HashSet<String> columns = new HashSet<>();
        for (Integer columnId : t.f1.columns) {
            String name = columnNames.get(columnId);
            if (name == null) {
                name = Integer.toString(columnId);
            }
            columns.add(name);
        }
        // emit a new record with the center id, the ucc decoded with column names and the similarity.
        return new Tuple3<>(t.f0, columns, t.f2);
    }
}
