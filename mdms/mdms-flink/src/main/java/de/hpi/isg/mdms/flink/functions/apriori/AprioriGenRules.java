package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

@SuppressWarnings("serial")
public final class AprioriGenRules extends RichFlatMapFunction<Tuple3<AssociationRule, Double, Double>, AssociationRule> {

    private Collection<Tuple3<AssociationRule, Double, Double>> rules_k_1;
    int k;

    public AprioriGenRules(int k) {
        this.k = k;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.rules_k_1 = getRuntimeContext().getBroadcastVariable("rules_k-1");
    }

    @Override
    public void flatMap(Tuple3<AssociationRule, Double, Double> rule, Collector<AssociationRule> out) throws Exception {

        for (Tuple3<AssociationRule, Double, Double> rule2 : this.rules_k_1) {
            if (rule2.f0.hasKCommonItemsWith(rule.f0)) {
                out.collect(rule.f0.generateWith(rule2.f0));
            }
        }
    }

}
