package de.hpi.isg.mdms.java.ml;

import org.apache.commons.lang3.Validate;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This class provides a logistic regression model.
 */
public class LogisticRegression {

    /**
     * Describes the loss function of logistic regression:
     * <p><i>sum over instances i: -label(i) * log(prediction(i)) - (1 - label(i)) * log(1 - prediction(i))</i></p>
     * The {@link VectorModel}'s parameters are coefficients to the corresponding features in feature vectors.
     */
    public static class LossDefinition<T> implements GradientDescent.LossDefinition<T> {

        @Override
        public double calculateLoss(VectorModel model, Collection<Observation<T>> observations) {
            double loss = 0d;
            for (Observation<?> observation : observations) {
                final double label = observation.getObservation();
                final double prediction = estimate(observation, model);
                loss -= label * Math.log(Math.max(prediction, Double.MIN_VALUE))
                        + (1 - label) * Math.log(Math.max(1 - prediction, Double.MIN_VALUE));
            }
            return loss;
        }

        @Override
        public double[] calculateGradient(VectorModel model, Collection<Observation<T>> observations) {
            double[] parameters = model.getParameters();
            double[] gradient = new double[parameters.length];
            for (Observation<?> observation : observations) {
                final double label = observation.getObservation();
                final double prediction = estimate(observation, model);
                final double[] featureVector = observation.getFeatureVector();
                for (int i = 0; i < featureVector.length; i++) {
                    gradient[i] += (prediction - label) * featureVector[i];
                }
                gradient[gradient.length - 1] += (prediction - label);
            }
            return gradient;
        }
    }

    public static <T> VectorModel train(Collection<Observation<T>> observations,
                                        int numFeatures,
                                        double learningRate,
                                        int numRepetitions,
                                        double minStepSize) {
        return GradientDescent.minimize(
                new LossDefinition<>(), observations, numFeatures + 1, learningRate, numRepetitions, minStepSize
        );
    }

    public static double estimate(Instance<?> instance, VectorModel model) {
        double[] parameters = model.getParameters();
        double[] features = instance.getFeatureVector();
        Validate.isTrue(parameters.length == features.length + 1);
        double linearResult = parameters[parameters.length - 1];
        for (int i = 0; i < features.length; i++) {
            linearResult += parameters[i] * features[i];
        }
        return sigmoid(linearResult);
    }

    public static <T> Collection<Prediction<T, Double>> estimateAll(Collection<Instance<T>> instances, VectorModel model) {
        return instances.stream()
                .map(instance -> new Prediction<>(instance, estimate(instance, model)))
                .collect(Collectors.toList());
    }

    private static double sigmoid(double v) {
        return 1 / (1 + Math.exp(-v));
    }

}
