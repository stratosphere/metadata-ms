package de.hpi.isg.mdms.java.feature;

import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;

import java.util.Collection;

/**
 * This interface represents the value update of a feature for all the {@link Instance} in the {@link Dataset}.
 * @author Lan Jiang
 */
public interface FeatureUpdate {

    /**
     * calculate the value of the feature for each instance in the dataset.
     * @param instanceCollection
     */
    public void calcualteFeatureValue(Collection<Instance> instanceCollection);
}
