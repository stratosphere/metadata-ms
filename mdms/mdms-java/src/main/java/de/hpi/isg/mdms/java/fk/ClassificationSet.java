package de.hpi.isg.mdms.java.fk;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.java.fk.classifiers.PartialForeignKeyClassifier;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A classification set aggregates a set of {@link PartialForeignKeyClassifier.WeightedResult}s pertaining to a single {@link InclusionDependency}.
 */
public class ClassificationSet {

    /**
     * The partial weighted results.
     */
    private final Collection<PartialForeignKeyClassifier.WeightedResult> partialResults = new LinkedList<>();

    /**
     * The {@link InclusionDependency} whose foreign key status is being classified.
     */
    private final UnaryForeignKeyCandidate fkCandidate;

    /**
     * This score aggregates the {@link #partialResults}.
     */
    private double score = Double.NaN;

    /**
     * Creates a new instance.
     *
     * @param fkCandidate the {@link InclusionDependency} whose foreign key status is to be classified
     */
    public ClassificationSet(UnaryForeignKeyCandidate fkCandidate) {
        this.fkCandidate = fkCandidate;
    }

    public void addPartialResult(PartialForeignKeyClassifier.WeightedResult weightedResult) {
        this.partialResults.add(weightedResult);
        this.score = Double.NaN;
    }

    public Collection<PartialForeignKeyClassifier.WeightedResult> getPartialResults() {
        return partialResults;
    }

    public UnaryForeignKeyCandidate getForeignKeyCandidate() {
        return fkCandidate;
    }

    /**
     * Computes and returns the {@link #score}, which aggregates the {@link #partialResults}.
     *
     * @return a score between {@code -1} (no foreign key) and {@code 1} (foreign key)
     */
    public double getOverallScore() {
        if (Double.isNaN(this.score)) calculateScore();
        return this.score;
    }

    private void calculateScore() {
        // Check for mandatory classifiers.
        final boolean isAllMandatoryTestsPassed = this.partialResults.stream()
                .filter(weightedResult -> Double.isNaN(weightedResult.getWeight()))
                .allMatch(weightedResult -> weightedResult.getLogicalResult() == PartialForeignKeyClassifier.Result.FOREIGN_KEY);
        if (!isAllMandatoryTestsPassed) {
            this.score = -1;
            return;
        }

        final List<PartialForeignKeyClassifier.WeightedResult> weightedResults = this.partialResults.stream()
                .filter(weightedResult -> !Double.isNaN(weightedResult.getWeight()))
                .collect(Collectors.toList());

        double weightSum = weightedResults.stream()
//                .filter(weightedResult -> weightedResult.getScore() != 0)
                .mapToDouble(PartialForeignKeyClassifier.WeightedResult::getWeight)
                .sum();
        double resultSum = weightedResults.stream()
                .mapToDouble(PartialForeignKeyClassifier.WeightedResult::getScore)
                .sum();

        this.score = weightSum == 0d ? 0d : resultSum / weightSum;
    }
}
