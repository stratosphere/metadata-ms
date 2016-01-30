package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.Item;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.ArrayList;

@SuppressWarnings("serial")
public final class UCCToTransactionsMapper extends RichMapFunction<Tuple1<int[]>, ItemSet> {

    @Override
    public ItemSet map(Tuple1<int[]> ucc) throws Exception {
        ArrayList<Item> items = new ArrayList<>();

        for (int column : ucc.f0) {
            items.add(new Item(column));
        }

        return new ItemSet(items);


    }
}
