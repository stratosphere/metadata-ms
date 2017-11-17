package de.hpi.isg.mdms.java.ml;

/**
 * This class describes the prediction result of a machine learning model on some {@link Instance}.
 */
public class Prediction<T, V> {

    private final Instance<T> instance;

    private final V prediction;

    public Prediction(Instance<T> instance, V prediction) {
        this.instance = instance;
        this.prediction = prediction;
    }

    public Instance<T> getInstance() {
        return instance;
    }

    public V getPrediction() {
        return prediction;
    }
}
