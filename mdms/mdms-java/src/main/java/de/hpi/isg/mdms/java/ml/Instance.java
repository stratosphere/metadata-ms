package de.hpi.isg.mdms.java.ml;

/**
 * Wraps some object and describes it as a feature vector.
 */
public class Instance<T> {

    private final double[] featureVector;

    private final T element;

    public Instance(T element, double[] featureVector) {
        this.element = element;
        this.featureVector = featureVector;
    }

    public double[] getFeatureVector() {
        return featureVector;
    }

    public T getElement() {
        return element;
    }
}
