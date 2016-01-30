package de.hpi.isg.mdms.flink.functions.kmeans;

import de.hpi.isg.mdms.flink.data.UCCCentroid;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Maps the cluster center ids to column names.
 */
public final class CentroidMapper extends RichMapFunction<UCCCentroid, Tuple2<Long, HashSet<String>>> {
    private HashMap<Integer, String> columnNames;

    public void setColumnNames(HashMap<Integer, String> map) {
        columnNames = map;
    }

    @Override
    public Tuple2<Long, HashSet<String>> map(UCCCentroid c) throws Exception {

        HashSet<String> columns = new HashSet<>();
        for (Integer columnId : c.columns) {
            String name = columnNames.get(columnId);
            if (name == null) {
                name = Integer.toString(columnId);
            }
            columns.add(name);
        }
        // emit a new record with the center id and the column names.
        return new Tuple2<>(c.id, columns);
    }
}
