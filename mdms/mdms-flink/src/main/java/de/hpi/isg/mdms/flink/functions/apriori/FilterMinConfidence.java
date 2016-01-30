package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("serial")
public final class FilterMinConfidence implements FilterFunction<Tuple3<AssociationRule, Double, Double>> {

    double minConfidence;

    public FilterMinConfidence(double minConfidence2) {
        this.minConfidence = minConfidence2;
    }

    @Override
    public boolean filter(Tuple3<AssociationRule, Double, Double> rule) throws Exception {
        return rule.f2 > this.minConfidence;
    }

}
