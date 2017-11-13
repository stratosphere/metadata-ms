package de.hpi.isg.mdms.java.ml;

/**
 * This class represents {@link Instance}s with observed dependent variables.
 */
public class Observation<T> extends Instance<T> {

    private final double observation;

    public Observation(T element, double[] featureVector, double observation) {
        super(element, featureVector);
        this.observation = observation;
    }

    public double getObservation() {
        return observation;
    }

}
