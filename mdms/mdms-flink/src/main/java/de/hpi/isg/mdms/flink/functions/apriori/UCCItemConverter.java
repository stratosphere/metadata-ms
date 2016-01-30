package de.hpi.isg.mdms.flink.functions.apriori;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public final class UCCItemConverter implements FlatMapFunction<Tuple1<int[]>, Tuple1<Integer>> {

    public void flatMap(Tuple1<int[]> ucc, Collector<Tuple1<Integer>> out) {
        for (int column : ucc.f0) {
            out.collect(new Tuple1<>(column));
        }
    }
}
