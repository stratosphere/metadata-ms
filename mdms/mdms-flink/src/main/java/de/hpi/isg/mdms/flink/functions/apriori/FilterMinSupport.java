package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@SuppressWarnings("serial")
public final class FilterMinSupport implements FilterFunction<Tuple2<ItemSet, Double>> {

    double minSupport;

    public FilterMinSupport(double minSupport2) {
        this.minSupport = minSupport2;
    }

    @Override
    public boolean filter(Tuple2<ItemSet, Double> item) throws Exception {

        return item.f1 > this.minSupport;
    }

}
