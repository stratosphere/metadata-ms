package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;

import java.util.Collection;

/**
 * Super class for various feature classes.
 * @author Lan Jiang
 */
abstract public class Feature {

    protected String featureName;

    /**
     * Indicate whether the feature is numeric or nominal.
     */
    protected String featureType;

    /**
     * Indicate the count of distinct value.
     */
    protected long distinctCount;

    /**
     * Indicate the unique value count in this dataset.
     */
    protected long uniqueCount;

    /**
     * Indicate the missing value count.
     */
    protected long missingCount;

    abstract public void calcualteFeatureValue(Collection<Instance> instanceCollection);

    abstract public void calculateFeatureValueDistribution(Dataset dataset);
}
