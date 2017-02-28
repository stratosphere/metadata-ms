package de.hpi.isg.mdms.java.feature;

/**
 * Super class for various feature classes.
 * @author Lan Jiang
 */
abstract public class Feature implements FeatureUpdate, NormalizationHandler {

    protected String featureName;

    /**
     * Indicate whether the feature is numeric or nominal.
     */
    protected FeatureType featureType;

    public enum FeatureType {
        Nominal,

        Numeric,

        String;
    }

    public String getFeatureName() {
        return featureName;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public void setFeatureType(FeatureType featureType) {
        this.featureType = featureType;
    }
}
