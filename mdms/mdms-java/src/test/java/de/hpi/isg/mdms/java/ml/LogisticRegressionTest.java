package de.hpi.isg.mdms.java.ml;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test suite for the {@link LogisticRegression} class.
 */
public class LogisticRegressionTest {

    @Test
    public void testEasyPredictions() {
        Collection<Observation<Void>> trainingData = Arrays.asList(
                new Observation<>(null, new double[]{-0d}, 0d),
                new Observation<>(null, new double[]{-1d}, 0d),
                new Observation<>(null, new double[]{-2d}, 0d),
                new Observation<>(null, new double[]{-3d}, 0d),
                new Observation<>(null, new double[]{-4d}, 0d),
                new Observation<>(null, new double[]{5d}, 1d),
                new Observation<>(null, new double[]{6d}, 1d),
                new Observation<>(null, new double[]{7d}, 1d),
                new Observation<>(null, new double[]{8d}, 1d),
                new Observation<>(null, new double[]{9d}, 1d)
        );

        VectorModel model = LogisticRegression.train(
                trainingData, 1, 1, 5, 0.001
        );

        for (Observation<Void> trainingDatum : trainingData) {
            System.out.println(LogisticRegression.predict(trainingDatum, model));
        }

    }
}
