package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;

import java.util.List;
import java.util.Objects;

/**
 * This constraint class encapsulates various general single column statistics.
 */
public class ColumnStatistics extends AbstractHashCodeAndEquals implements Constraint {

    private long numNulls = -1, numDistinctValues = -1;

    private double fillStatus = Double.NaN, uniqueness = Double.NaN;

    public List<ColumnStatistics.ValueOccurrence> topKFrequentValues;

    private final int columnId;

    public ColumnStatistics(int columnId) {
        this.columnId = columnId;
    }

    public long getNumNulls() {
        return numNulls;
    }

    public void setNumNulls(long numNulls) {
        this.numNulls = numNulls;
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public void setNumDistinctValues(long numDistinctValues) {
        this.numDistinctValues = numDistinctValues;
    }

    public double getFillStatus() {
        return fillStatus;
    }

    public void setFillStatus(double fillStatus) {
        this.fillStatus = fillStatus;
    }

    public double getUniqueness() {
        return uniqueness;
    }

    public void setUniqueness(double uniqueness) {
        this.uniqueness = uniqueness;
    }

    public List<ColumnStatistics.ValueOccurrence> getTopKFrequentValues() {
        return topKFrequentValues;
    }

    public void setTopKFrequentValues(List<ColumnStatistics.ValueOccurrence> topKFrequentValues) {
        this.topKFrequentValues = topKFrequentValues;
    }

    public int getColumnId() {
        return this.columnId;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.columnId};
    }

    /**
     * This class describes a value and the number of its occurrences (in a column). Instances are primarily ordered by
     * their count and by their value as tie breaker.
     */
    public static class ValueOccurrence implements Comparable<ColumnStatistics.ValueOccurrence> {

        private final String value;

        private final long numOccurrences;

        public ValueOccurrence(String value, long numOccurrences) {
            this.value = value;
            this.numOccurrences = numOccurrences;
        }

        public String getValue() {
            return value;
        }

        public long getNumOccurrences() {
            return numOccurrences;
        }

        @Override
        public int compareTo(ColumnStatistics.ValueOccurrence that) {
            int result = Long.compare(this.getNumOccurrences(), that.getNumOccurrences());
            return result == 0 ? this.getValue().compareTo(that.getValue()) : result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;
            final ColumnStatistics.ValueOccurrence that = (ColumnStatistics.ValueOccurrence) o;
            return this.numOccurrences == that.numOccurrences &&
                    Objects.equals(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.value, this.numOccurrences);
        }
    }

}
