package de.hpi.isg.mdms.flink.functions.apriori;

import de.hpi.isg.mdms.flink.data.apriori.Item;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import org.apache.flink.api.common.functions.RichMapFunction;

@SuppressWarnings("serial")
public final class ItemToItemSet extends RichMapFunction<Item, ItemSet> {

    @Override
    public ItemSet map(Item item) throws Exception {

        return new ItemSet(item);
    }
}
