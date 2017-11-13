package de.hpi.isg.mdms.java.ml;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

/**
 * Test suite for the {@link GradientDescent} class.
 */
public class GradientDescentTest {

    /**
     * In this test setup, we use a loss function that simply sums up squares of all values in the parameter vector and
     * that neglects all observations. As a result, the vector with all {@code 0}s is the only local (and thus the global
     * minimum of the loss function). This minimum should be found.
     */
    @Test
    public void testFindSingleLocalMinimum() {
        final int numDimension = 10;
        GradientDescent.LossDefinition lossDefinition = new GradientDescent.LossDefinition() {
            @Override
            public double calculateLoss(VectorModel model, Collection<Observation<?>> observations) {
                double loss = 0d;
                double[] parameters = model.getParameters();
                for (int i = 0; i < parameters.length; i++) {
                    double v = parameters[i];
                    loss += v * v;
                }
                return loss;
            }

            @Override
            public double[] calculateGradient(VectorModel model, Collection<Observation<?>> observations) {
                double[] gradient = new double[numDimension];
                double[] parameters = model.getParameters();
                for (int i = 0; i < parameters.length; i++) {
                    double v = parameters[i];
                    gradient[i] = 2 * v; // (x^2 + k)' = 2x
                }
                return gradient;
            }
        };

        Collection<Observation<?>> observations = Collections.emptyList();
        VectorModel model = GradientDescent.minimize(lossDefinition, observations, numDimension, 0.1, 1, 0.0001);
        double loss = lossDefinition.calculateLoss(model, observations);

        double[] parameters = model.getParameters();
        Assert.assertEquals(numDimension, parameters.length);
        Assert.assertEquals(0, loss, 0.01);
        for (int i = 0; i < parameters.length; i++) {
            double v = parameters[i];
            Assert.assertEquals(0d, v, 0.01);
        }
    }

    /**
     * In this test setup, we use a loss function that simply sums up squares of all values minus the dimension number in the parameter vector and
     * that neglects all observations. As a result, the vector with each value equaling its dimension number is the only local (and thus the global
     * minimum of the loss function). This minimum should be found.
     */
    @Test
    public void testFindSingleLocalMinimum2() {
        final int numDimension = 10;
        GradientDescent.LossDefinition lossDefinition = new GradientDescent.LossDefinition() {
            @Override
            public double calculateLoss(VectorModel model, Collection<Observation<?>> observations) {
                double loss = 0d;
                double[] parameters = model.getParameters();
                for (int i = 0; i < parameters.length; i++) {
                    double v = parameters[i] - i;
                    loss += v * v;
                }
                return loss;
            }

            @Override
            public double[] calculateGradient(VectorModel model, Collection<Observation<?>> observations) {
                double[] gradient = new double[numDimension];
                double[] parameters = model.getParameters();
                for (int i = 0; i < parameters.length; i++) {
                    double v = parameters[i];
                    gradient[i] = 2 * (v - i); // ((x-i)^2 + k)' = (x^2 - 2ix + i^2 + k)' = 2x - 2i
                }
                return gradient;
            }
        };

        Collection<Observation<?>> observations = Collections.emptyList();
        VectorModel model = GradientDescent.minimize(lossDefinition, observations, numDimension, 0.1, 1, 0.0001);
        double loss = lossDefinition.calculateLoss(model, observations);

        double[] parameters = model.getParameters();
        Assert.assertEquals(numDimension, parameters.length);
        Assert.assertEquals(0, loss, 0.01);
        for (int i = 0; i < parameters.length; i++) {
            double v = parameters[i];
            Assert.assertEquals(i, v, 0.01);
        }
    }

}
