package de.hpi.isg.mdms.java.util;

/**
 * This class represents a foreign key candidate between two singe columns.
 */
public class UnaryForeignKeyCandidate {

    /**
     * ID of the dependent column.
     */
    private final int dependentColumnId;


    /**
     * ID of the referenced column.
     */
    private final int referencedColumnId;

    public UnaryForeignKeyCandidate(int dependentColumnId, int referencedColumnId) {
        this.dependentColumnId = dependentColumnId;
        this.referencedColumnId = referencedColumnId;
    }

    public int getDependentColumnId() {
        return this.dependentColumnId;
    }

    public int getReferencedColumnId() {
        return this.referencedColumnId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UnaryForeignKeyCandidate that = (UnaryForeignKeyCandidate) o;

        return this.dependentColumnId == that.dependentColumnId && this.referencedColumnId == that.referencedColumnId;

    }

    @Override
    public int hashCode() {
        int result = this.dependentColumnId;
        result = 31 * result + this.referencedColumnId;
        return result;
    }

    @Override
    public String toString() {
        return "UnaryForeignKeyCandidate{" +
                "dependentColumnId=" + this.dependentColumnId +
                ", referencedColumnId=" + this.referencedColumnId +
                '}';
    }
}
