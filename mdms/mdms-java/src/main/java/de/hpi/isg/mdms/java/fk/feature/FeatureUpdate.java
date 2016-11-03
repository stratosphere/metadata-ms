package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Instance;

import java.util.Collection;

/**
 * This interface represents the value update of a feature for all the {@link Instance} in the {@link de.hpi.isg.mdms.java.fk.Dataset}.
 * @author Lan Jiang
 */
public interface FeatureUpdate {

    /**
     * calculate the value of the feature for each instance in the dataset.
     * @param instanceCollection
     */
    public void calcualteFeatureValue(Collection<Instance> instanceCollection);
}
