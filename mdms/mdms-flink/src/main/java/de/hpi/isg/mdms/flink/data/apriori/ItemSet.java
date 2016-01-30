package de.hpi.isg.mdms.flink.data.apriori;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;

/**
 * An itemset
 */
public class ItemSet {

    public ArrayList<Item> items;

    public ItemSet() {
        this.items = new ArrayList<>();
    }

    public ItemSet(ArrayList<Item> itemList) {
        this.items = itemList;
    }

    public ArrayList<AssociationRule> getAssociationRules() {
        ArrayList<AssociationRule> rules = new ArrayList<AssociationRule>();
        for (Item rhs : this.items) {
            ArrayList<Item> lhs = new ArrayList<Item>();
            lhs.addAll(this.items);
            lhs.remove(rhs);
            AssociationRule rule = new AssociationRule(new ItemSet(lhs), rhs);
            rules.add(rule);
        }

        return rules;
    }

    public ItemSet generateWith(ItemSet itemSet) {
        ArrayList<Item> newItems = new ArrayList<>();
        newItems.addAll(this.items);
        for (Item item : itemSet.items) {
            if (!newItems.contains(item)) {
                newItems.add(item);
            }
        }

        return new ItemSet(newItems);
    }

    public boolean hasKCommonItemsWith(ItemSet itemSet) {
        int k = 0;
        for (Item item : itemSet.items) {
            if (!this.items.contains(item)) {
                k++;
            }
        }

        return k == 1;

    }

    public ItemSet(Item item) {
        this.items = new ArrayList<>();
        this.items.add(item);
    }

    public boolean contains(Item item) {
        return this.items.contains(item);
    }

    public boolean contains(ItemSet itemSet) {
        for (Item item : itemSet.items) {
            if (!this.items.contains(item)) return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        ItemSet i = (ItemSet) o;
        return (this.contains(i) && i.contains(this));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(this.items)
                .toHashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ItemSet (");
        for (int i = 0; i < (this.items.size() - 1); i++) {
            sb.append(this.items.get(i).id);
            sb.append(", ");
        }
        sb.append(this.items.get(this.items.size() - 1).id);
        sb.append(")");

        return sb.toString();
    }

}
