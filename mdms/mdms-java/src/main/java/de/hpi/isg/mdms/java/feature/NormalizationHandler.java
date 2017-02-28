package de.hpi.isg.mdms.java.feature;

import de.hpi.isg.mdms.java.util.Instance;
import de.hpi.isg.mdms.java.util.Dataset;

import java.util.Collection;

/**
 * An interface to supply the normalize function for the features.
 * @author Lan Jiang
 * @since 11/02/2017
 */
public interface NormalizationHandler {

    /**
     * Normalize the feature.
     * @param valueForNormalizing the value that needs to be normalized
     * @return normalized value
     */
    double normalize(double valueForNormalizing);
}
