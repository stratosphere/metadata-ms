package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

@SuppressWarnings("serial")
public class CompareAssociationRulesReduce implements GroupReduceFunction<Tuple3<AssociationRule, Double, Double>, Tuple3<AssociationRule, Double, Double>> {

    @Override
    public void reduce(Iterable<Tuple3<AssociationRule, Double, Double>> in, Collector<Tuple3<AssociationRule, Double, Double>> out) {

        ArrayList<AssociationRule> rules = new ArrayList<AssociationRule>();

        for (Tuple3<AssociationRule, Double, Double> i : in) {
            boolean alreadyOutput = false;
            for (AssociationRule rule : rules) {
                if (rule.equals(i.f0)) {
                    alreadyOutput = true;
                }
            }
            if (!alreadyOutput) {
                out.collect(i);
                rules.add(i.f0);
            }
        }


    }
}
