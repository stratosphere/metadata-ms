package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;

/**
 * Maps the datapoints to uccDatapoints.
 */
@SuppressWarnings("serial")
public final class RuleMapper extends RichMapFunction<Tuple3<AssociationRule, Double, Double>, Tuple3<String, Double, Double>> {

    private HashMap<Integer, String> columnNames;

    public void setColumnNames(HashMap<Integer, String> map) {
        this.columnNames = map;
    }

    @Override
    public Tuple3<String, Double, Double> map(Tuple3<AssociationRule, Double, Double> rule) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("Rule (");

        for (int i = 0; i < (rule.f0.lhs.items.size() - 1); i++) {
            sb.append(this.columnNames.get(rule.f0.lhs.items.get(i).id));
            sb.append(", ");
        }
        sb.append(this.columnNames.get(rule.f0.lhs.items.get(rule.f0.lhs.items.size() - 1).id));
        sb.append(") --> ");
        sb.append(this.columnNames.get(rule.f0.rhs.id));

        return new Tuple3<>(sb.toString(), rule.f1, rule.f2);
    }
}
