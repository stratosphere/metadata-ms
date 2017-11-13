package de.hpi.isg.mdms.java.ml;

import org.apache.commons.lang3.Validate;

/**
 * A vector model is a model of a machine learning algorithm that is represented as a vector of numbers (called parameters).
 */
public class VectorModel {

    private final double[] parameters;

    public VectorModel(double[] parameters) {
        this.parameters = parameters;
    }

    /**
     * Alter this model by summing the {@code delta} vector with this instance's parameter vector.
     *
     * @param delta the delta vector
     */
    public void add(double[] delta) {
        Validate.isTrue(this.parameters.length == delta.length);
        for (int i = 0; i < delta.length; i++) {
            this.parameters[i] += delta[i];
        }
    }

    public double[] getParameters() {
        return parameters;
    }
}
