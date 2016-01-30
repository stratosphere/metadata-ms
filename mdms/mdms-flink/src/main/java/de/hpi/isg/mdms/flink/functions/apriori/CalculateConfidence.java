package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

@SuppressWarnings("serial")
public final class CalculateConfidence extends RichMapFunction<Tuple2<AssociationRule, Double>, Tuple3<AssociationRule, Double, Double>> {

    private Collection<Tuple2<ItemSet, Double>> candidates;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.candidates = getRuntimeContext().getBroadcastVariable("candidates");
    }

    @Override
    public Tuple3<AssociationRule, Double, Double> map(Tuple2<AssociationRule, Double> rule) throws Exception {

        double supportLHS = 0.0;

        for (Tuple2<ItemSet, Double> candidate : this.candidates) {
            if (rule.f0.lhs.equals(candidate.f0)) {
                supportLHS = candidate.f1;
                break;
            }
        }

        double confidence = rule.f1 / supportLHS;
        return new Tuple3<AssociationRule, Double, Double>(rule.f0, rule.f1, confidence);
    }
}
