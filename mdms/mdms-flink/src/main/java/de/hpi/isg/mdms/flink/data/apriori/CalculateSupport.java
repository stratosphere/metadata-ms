package de.hpi.isg.mdms.flink.data.apriori;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

@SuppressWarnings("serial")
public final class CalculateSupport extends RichMapFunction<ItemSet, Tuple2<ItemSet, Double>> {
    private Collection<ItemSet> transactions;

    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.transactions = getRuntimeContext().getBroadcastVariable("transactions");
    }

    @Override
    public Tuple2<ItemSet, Double> map(ItemSet iSet) throws Exception {

        double occurences = 0.0;

        for (ItemSet transaction : this.transactions) {
            if (transaction.contains(iSet)) {
                occurences++;
            }
        }
        double support = occurences / this.transactions.size();
        return new Tuple2<>(iSet, support);
    }
}
