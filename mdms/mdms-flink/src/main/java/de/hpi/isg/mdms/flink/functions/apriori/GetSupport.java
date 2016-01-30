package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import de.hpi.isg.mdms.flink.data.apriori.Item;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collection;

@SuppressWarnings("serial")
public final class GetSupport extends RichMapFunction<AssociationRule, Tuple2<AssociationRule, Double>> {

    private Collection<Tuple2<ItemSet, Double>> candidates;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.candidates = getRuntimeContext().getBroadcastVariable("candidates");
    }

    @Override
    public Tuple2<AssociationRule, Double> map(AssociationRule rule) throws Exception {

        double support = 0.0;
        ArrayList<Item> itemList = new ArrayList<Item>();
        itemList.addAll(rule.lhs.items);
        itemList.add(rule.rhs);
        for (Tuple2<ItemSet, Double> candidate : this.candidates) {
            if (candidate.f0.equals(new ItemSet(itemList))) {
                support = candidate.f1;
                break;
            }
        }

        return new Tuple2<AssociationRule, Double>(rule, support);
    }
}
