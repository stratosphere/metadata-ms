package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.Item;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class TakeFirstItemReduce implements GroupReduceFunction<Tuple1<Integer>, Item> {

    @Override
    public void reduce(Iterable<Tuple1<Integer>> in, Collector<Item> out) {
        for (Tuple1<Integer> i : in) {
            out.collect(new Item(i.f0));
            return;
        }
    }
}
