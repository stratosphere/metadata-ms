package de.hpi.isg.mdms.java.ml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;

/**
 * Gradient descent is an algorithm to minimize a loss function that is differentiable.
 */
public class GradientDescent {

    private static final Logger logger = LoggerFactory.getLogger(LogisticRegression.class);

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
            logger.info("Performing repetition {}/{}.", repetition + 1, numRepetitions);

            // Generate an initial model.
            final double[] parameters = new double[dimensionality];
            for (int dimension = 0; dimension < parameters.length; dimension++) {
                parameters[dimension] = random.nextGaussian() * 10; // TODO: It might be important to allow a meaningful customization of the initial weights.
            }
            final VectorModel model = new VectorModel(parameters);

            // Repeatedly go against the gradient until convergence.
            double stepSize;
            double lastLoss = Double.NaN;
            long nextLogMillis = System.currentTimeMillis() + 30_000L;
            long round = 1L;
            do {
                // Calculate the gradient of the current model.
                final double[] gradient = lossDefinition.calculateGradient(model, observations);

                // Update the model and calculate the width of the update step.
                for (int i = 0; i < gradient.length; i++) {
                    gradient[i] *= -learningRate;
                }
                model.add(gradient);
                stepSize = calculateEuclidianDistance(gradient);

                // Adapt the learning rate (Bold Driver):
                // If we we are getting better, increase the learning rate by 5%. Otherwise, decrease it by 50%.
                double loss = lossDefinition.calculateLoss(model, observations);
                if (!Double.isNaN(lastLoss)) {
                    if (lastLoss >= loss) learningRate *= 1.05;
                    else learningRate *= .5;
                }
                lastLoss = loss;

                if (System.currentTimeMillis() >= nextLogMillis) {
                    logger.info("Current loss after {} rounds: {}", round, loss);
                    nextLogMillis += 30_000L;
                }
                round++;
            } while (stepSize > minStepSize);

            // Update the best model.
            double loss = lossDefinition.calculateLoss(model, observations);
            logger.info("Final loss after {} rounds: {}", round, loss);
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
