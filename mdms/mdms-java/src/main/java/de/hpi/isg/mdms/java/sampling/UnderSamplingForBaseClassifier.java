package de.hpi.isg.mdms.java.sampling;

import weka.classifiers.Classifier;

/**
 * @author Lan Jiang
 * @since 10/02/2017
 */
public interface UnderSamplingForBaseClassifier {
    public Classifier[] getClassifier();
}
