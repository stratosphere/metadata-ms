package de.hpi.isg.mdms.java.fk.ml.classifier;

/**
 * This interface represent the operations that a concrete classifier may have, i.e. train and predict.
 * @author Lan Jiang
 */
public interface Classifier {

    public void train();

    public void predict();
}
