package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

@SuppressWarnings("serial")
public final class AprioriGen extends RichFlatMapFunction<Tuple2<ItemSet, Double>, ItemSet> {

    private Collection<Tuple2<ItemSet, Double>> candidates_k_1;
    int k;

    public AprioriGen(int k) {
        this.k = k;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.candidates_k_1 = getRuntimeContext().getBroadcastVariable("candidates_k-1");
    }

    @Override
    public void flatMap(Tuple2<ItemSet, Double> candidate, Collector<ItemSet> out) throws Exception {

        for (Tuple2<ItemSet, Double> candidate_2 : this.candidates_k_1) {
            if (candidate.f0.hasKCommonItemsWith(candidate_2.f0)) {
                out.collect(candidate.f0.generateWith(candidate_2.f0));
            }
        }
    }

}
