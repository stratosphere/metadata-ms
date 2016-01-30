package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

@SuppressWarnings("serial")
public class CompareItemSetReduce implements GroupReduceFunction<Tuple2<ItemSet, Double>, Tuple2<ItemSet, Double>> {

    @Override
    public void reduce(Iterable<Tuple2<ItemSet, Double>> in, Collector<Tuple2<ItemSet, Double>> out) {
        ArrayList<ItemSet> itemSets = new ArrayList<ItemSet>();

        for (Tuple2<ItemSet, Double> i : in) {
            boolean alreadyOutput = false;
            for (ItemSet itemSet : itemSets) {
                if (itemSet.contains(i.f0)) {
                    alreadyOutput = true;
                }
            }
            if (!alreadyOutput) {
                out.collect(i);
                itemSets.add(i.f0);
            }
        }
    }
}
