package de.hpi.isg.mdms.java.ml;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * Gradient descent is an algorithm to minimize a loss function that is differentiable.
 */
public class GradientDescent {

    /**
     * Describes a type of loss function to be minimized. The actual loss function is only instantiated when given
     * {@link Observation}s and a {@link VectorModel}.
     */
    public interface LossDefinition<T> {

        /**
         * Calculate the loss of a certain {@code model} with the given {@code observations}.
         *
         * @param model        the model to evaluate
         * @param observations the data to evaluate on
         * @return the loss
         */
        double calculateLoss(VectorModel model, Collection<Observation<T>> observations);

        /**
         * Calculate the loss function's gradient of a certain {@code model} with the given {@code observations}.
         *
         * @param model        the model whose gradient we look for w.r.t. this loss function
         * @param observations the data to evaluate on
         * @return the gradient
         */
        double[] calculateGradient(VectorModel model, Collection<Observation<T>> observations);

    }

    /**
     * Minimize a loss function.
     *
     * @param lossDefinition defines the type of loss function
     * @param observations   defines the data on which to calculate the loss
     * @param dimensionality the number of dimensions of the loss functions parameter vector
     * @param learningRate   the learning rate for the minimization
     * @param numRepetitions how often to restart from a random
     * @return a {@link VectorModel} that represents the best discovered minimum
     */
    public static <T> VectorModel minimize(LossDefinition<T> lossDefinition,
                                           Collection<Observation<T>> observations,
                                           int dimensionality,
                                           double learningRate,
                                           int numRepetitions,
                                           double minStepSize) {

        final Random random = new Random();

        // Repeat the optimization multiple times and keep the best model.
        VectorModel bestModel = null;
        double bestLoss = Double.NaN;
        for (int repetition = 0; repetition < numRepetitions; repetition++) {
            // Generate an initial model.
            final double[] parameters = new double[dimensionality];
            for (int dimension = 0; dimension < parameters.length; dimension++) {
                parameters[dimension] = random.nextInt(); // We don't use nextDouble(), which generates number only from [0, 1).
            }
            final VectorModel model = new VectorModel(parameters);

            // Repeatedly go against the gradient until convergence.
            double stepSize;
            double lastLoss = Double.NaN;
            do {
                // Calculate the gradient of the current model.
                final double[] gradient = lossDefinition.calculateGradient(model, observations);

                // Update the model and calculate the width of the update step.
                for (int i = 0; i < gradient.length; i++) {
                    gradient[i] *= -learningRate;
                }
                model.add(gradient);
                stepSize = calculateEuclidianDistance(gradient);
                System.out.printf("Loss: %+10.10f\tGradient: %s\tModel: %s\n", lossDefinition.calculateLoss(model, observations), Arrays.toString(gradient), Arrays.toString(model.getParameters()));

                // Adapt the learning rate (Bold Driver):
                // If we we are getting better, increase the learning rate by 5%. Otherwise, decrease it by 50%.
                double loss = lossDefinition.calculateLoss(model, observations);
                if (!Double.isNaN(lastLoss)) {
                    if (lastLoss >= loss) learningRate *= 1.05;
                    else learningRate *= .5;
                }
                lastLoss = loss;
            } while (stepSize > minStepSize);

            // Update the best model.
            double loss = lossDefinition.calculateLoss(model, observations);
            if (bestModel == null || bestLoss < loss) {
                bestModel = model;
                bestLoss = loss;
            }
        }

        return bestModel;
    }

    private static double calculateEuclidianDistance(double[] vector) {
        double dotSelfProduct = 0d;
        for (int i = 0; i < vector.length; i++) {
            double v = vector[i];
            dotSelfProduct += v * v;
        }
        return Math.sqrt(dotSelfProduct);
    }

}
