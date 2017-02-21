package de.hpi.isg.mdms.domain.constraints;

/**
 * This constraint class encapsulates string-specific single column statistics.
 */
public class TextColumnStatistics implements RDBMSConstraint {

    /**
     * Special values in a column.
     */
    private String minValue, maxValue, shortestValue, longestValue;

    /**
     * The type of strings contained in a column, such as JSON or UUID.
     */
    private String subtype;

    private final SingleTargetReference targetReference;

    public TextColumnStatistics(int columnId) {
        this.targetReference = new SingleTargetReference(columnId);
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.targetReference;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public String getShortestValue() {
        return shortestValue;
    }

    public void setShortestValue(String shortestValue) {
        this.shortestValue = shortestValue;
    }

    public String getLongestValue() {
        return longestValue;
    }

    public void setLongestValue(String longestValue) {
        this.longestValue = longestValue;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }

}
