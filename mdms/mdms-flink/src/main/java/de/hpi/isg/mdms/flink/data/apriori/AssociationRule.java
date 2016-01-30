package de.hpi.isg.mdms.flink.data.apriori;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * An association rule.
 */
public class AssociationRule {

    public ItemSet lhs;
    public Item rhs;

    public AssociationRule(ItemSet lhs, Item rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public AssociationRule generateWith(AssociationRule rule) {

        ItemSet newLHS = this.lhs.generateWith(rule.lhs);
        return new AssociationRule(newLHS, this.rhs);
    }


    public boolean hasKCommonItemsWith(AssociationRule rule) {

        return this.rhs.equals(rule.rhs) && this.lhs.hasKCommonItemsWith(rule.lhs);

    }

    @Override
    public boolean equals(Object o) {
        AssociationRule i = (AssociationRule) o;
        return (i.lhs.equals(this.lhs) && i.rhs.equals(this.rhs));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(this.lhs)
                .append(this.rhs)
                .toHashCode();
    }

    @Override
    public String toString() {
        return this.lhs + " --> " + this.rhs;
    }

}
