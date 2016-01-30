package de.hpi.isg.mdms.flink.data.apriori;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * An item.
 */
public class Item {

    public int id;

    public Item(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        Item i = (Item) o;
        return (i.id == this.id);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(this.id)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "Item " + this.id;
    }

}
