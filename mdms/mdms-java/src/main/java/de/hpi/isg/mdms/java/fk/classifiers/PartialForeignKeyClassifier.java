package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.java.apps.ForeignKeyClassifier;
import de.hpi.isg.mdms.java.fk.ClassificationSet;

import java.util.Collection;

/**
 * This class defines the interface of a partial foreign key classifier that can be employed by
 * {@link ForeignKeyClassifier}.
 */
abstract public class PartialForeignKeyClassifier {

    /**
     * Weight of this classifier.
     */
    private final double weight;

    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results or {@link Double#NaN} to make this a mandatory classifier.
     */
    protected PartialForeignKeyClassifier(double weight) {
        this.weight = weight;
    }

    /**
     * Classifies a set of {@link InclusionDependency} objects.
     *
     * @param classificationSets a {@link ClassificationSet} for each {@link InclusionDependency} to be considered;
     *                           also takes the classification result
     */
    abstract public void classify(Collection<ClassificationSet> classificationSets);


    /**
     * Possible results of a partial foreign key classifier.
     */
    public enum Result {
        /**
         * Indicates that the classifier believes that an IND is a foreign key.
         */
        FOREIGN_KEY,

        /**
         * Indicates that the classifier is not sure whether an IND is a foreign key.
         */
        UNKNOWN,

        /**
         * Indicates that the classifier believes that an IND is not a foreign key.
         */
        NO_FOREIGN_KEY;

        /**
         * Logical AND of two {@link Result} values using ternary logic.
         */
        public static Result and(Result a, Result b) {
            return Result.values()[Math.max(a.ordinal(), b.ordinal())];
        }

        /**
         * @return the unweighted score of this unweighted result
         */
        public int getScore() {
            return 1 - ordinal();
        }
    }

    /**
     * This class amends a logical {@link Result} with a weight.
     */
    public class WeightedResult {

        private final Result logicalResult;

        /**
         * Creates a new instance.
         *
         * @param logicalResult the logical result
         */
        public WeightedResult(Result logicalResult) {
            this.logicalResult = logicalResult;
        }

        public double getWeight() {
            return PartialForeignKeyClassifier.this.weight;
        }

        public Result getLogicalResult() {
            return this.logicalResult;
        }

        /**
         * Returns the weighted score of this result
         *
         * @return the weighted score of this result
         */
        public double getScore() {
            return PartialForeignKeyClassifier.this.weight * this.logicalResult.getScore();
        }

        @Override
        public String toString() {
            return String.format("%s(%f*%s)",
                    PartialForeignKeyClassifier.this.getClass().getSimpleName(),
                    PartialForeignKeyClassifier.this.weight, this.logicalResult);
        }
    }

}
