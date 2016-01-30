package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

@SuppressWarnings("serial")
public final class MapCandidateToAssociationRule extends RichFlatMapFunction<Tuple2<ItemSet, Double>, Tuple2<AssociationRule, Double>> {

    @Override
    public void flatMap(Tuple2<ItemSet, Double> candidate, Collector<Tuple2<AssociationRule, Double>> out) throws Exception {

        ArrayList<AssociationRule> rules = candidate.f0.getAssociationRules();
        for (AssociationRule rule : rules) {
            out.collect(new Tuple2<>(rule, candidate.f1));
        }
    }
}
