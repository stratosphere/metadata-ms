package de.hpi.isg.mdms.domain.constraints;

/**
 * This constraint class encapsulates string-specific single column statistics.
 */
public class NumberColumnStatistics implements RDBMSConstraint {

    /**
     * Special values in a column.
     */
    private double minValue = Double.NaN, maxValue = Double.NaN;

    /**
     * The standard deviation of values in a column.
     */
    private double standardDeviation = Double.NaN;

    /**
     * The average of values in a column.
     */
    private double average = Double.NaN;

    /**
     * Reference to the described column.
     */
    private final SingleTargetReference targetReference;

    /**
     * Create a new instance.
     *
     * @param columnId the ID of the column that is to be described by the new instance
     */
    public NumberColumnStatistics(int columnId) {
        this.targetReference = new SingleTargetReference(columnId);
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.targetReference;
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

}
