package de.hpi.isg.mdms.java.schema_inference;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.TextColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.java.ml.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class models foreign key detection as a machine learning problem.
 */
public class ForeignKeys {

    /**
     * Create a training set for foreign key classification.
     *
     * @param store                 where all metadata resides in
     * @param foreignKeyCC          known foreign keys
     * @param indCC                 all {@link InclusionDependency}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link InclusionDependency} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link InclusionDependency} columns
     * @return the training dataset consisting of {@link Observation}s
     */
    public static Collection<Observation<InclusionDependency>> createTrainingSet(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> foreignKeyCC,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC) {
        // Index the constraint collections.
        Map<Integer, ColumnStatistics> columnStatistics = columnStatisticCC.getConstraints().stream()
                .collect(Collectors.toMap(
                        ColumnStatistics::getColumnId,
                        Function.identity()
                ));
        Map<Integer, TextColumnStatistics> textColumnStatistics = textColumnStatisticCC.getConstraints().stream()
                .collect(Collectors.toMap(
                        TextColumnStatistics::getColumnId,
                        Function.identity()
                ));
        Set<InclusionDependency> foreignKeys = new HashSet<>(foreignKeyCC.getConstraints());

        return indCC.getConstraints().stream()
                .map(ind -> new Observation<>(
                        ind,
                        createFeatureVector(ind, store, columnStatistics, textColumnStatistics),
                        foreignKeys.contains(ind) ? 1d : 0d
                ))
                .collect(Collectors.toList());
    }

    /**
     * Create a dataset that is ready for foreign key classification.
     *
     * @param store                 where all metadata resides in
     * @param indCC                 all {@link InclusionDependency}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link InclusionDependency} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link InclusionDependency} columns
     * @return the estimation dataset consisting of {@link Instance}s
     */
    public static Collection<Instance<InclusionDependency>> createEstimationSet(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC) {
        // Index the constraint collections.
        Map<Integer, ColumnStatistics> columnStatistics = columnStatisticCC.getConstraints().stream()
                .collect(Collectors.toMap(
                        ColumnStatistics::getColumnId,
                        Function.identity()
                ));
        Map<Integer, TextColumnStatistics> textColumnStatistics = textColumnStatisticCC.getConstraints().stream()
                .collect(Collectors.toMap(
                        TextColumnStatistics::getColumnId,
                        Function.identity()
                ));

        return indCC.getConstraints().stream()
                .map(ind -> new Instance<>(
                        ind,
                        createFeatureVector(ind, store, columnStatistics, textColumnStatistics)
                ))
                .collect(Collectors.toList());
    }

    /**
     * Balance a training set (as per {@link #createTrainingSet(MetadataStore, ConstraintCollection, ConstraintCollection, ConstraintCollection, ConstraintCollection)}
     * via oversampling.
     *
     * @param instances that should be balanced
     * @return a balanced training dataset
     */
    public static <T> Collection<Observation<T>> oversample(
            Collection<Observation<T>> instances) {
        List<Observation<T>> positiveInstances = new ArrayList<>();
        List<Observation<T>> negativeInstances = new ArrayList<>();
        for (Observation<T> instance : instances) {
            (instance.getObservation() == 0d ? negativeInstances : positiveInstances).add(instance);
        }

        int p = positiveInstances.size(), n = negativeInstances.size();
        final Random random = new Random();
        while (!positiveInstances.isEmpty() && positiveInstances.size() < negativeInstances.size()) {
            positiveInstances.add(positiveInstances.get(random.nextInt(p)));
        }
        while (!negativeInstances.isEmpty() && positiveInstances.size() > negativeInstances.size()) {
            negativeInstances.add(negativeInstances.get(random.nextInt(n)));
        }

        positiveInstances.addAll(negativeInstances);
        return positiveInstances;
    }

    /**
     * Train a logistic regression model for the classification of foreign keys.
     *
     * @param store                 where all metadata resides in
     * @param foreignKeyCC          known foreign keys
     * @param indCC                 all {@link InclusionDependency}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link InclusionDependency} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link InclusionDependency} columns
     * @return the {@link VectorModel} for the trained logistic regression model
     */
    public static VectorModel trainLogisticRegressionModel(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> foreignKeyCC,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC) {
        Collection<Observation<InclusionDependency>> trainingSet =
                createTrainingSet(store, foreignKeyCC, indCC, columnStatisticCC, textColumnStatisticCC);
        trainingSet = oversample(trainingSet);
        return LogisticRegression.train(trainingSet, numFeatures, 1d, 5, 0.0001);
    }

    /**
     * Estimate the probability of {@link InclusionDependency}s to be foreign keys.
     *
     * @param store                   where all metadata resides in
     * @param indCC                   all {@link InclusionDependency}s
     * @param columnStatisticCC       {@link ColumnStatistics} for the {@link InclusionDependency} columns
     * @param textColumnStatisticCC   {@link TextColumnStatistics} for the {@link InclusionDependency} columns
     * @param logisticRegressionModel a logistic regression model for the estimates
     * @return one {@link Prediction} per input {@link InclusionDependency}
     */
    public static Collection<Prediction<InclusionDependency, Double>> estimateForeignKeyProbability(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            VectorModel logisticRegressionModel) {
        Collection<Instance<InclusionDependency>> estimationSet = createEstimationSet(store, indCC, columnStatisticCC, textColumnStatisticCC);
        return LogisticRegression.estimateAll(estimationSet, logisticRegressionModel);
    }

    /**
     * Select potential foreign keys among {@link InclusionDependency}s based on their estimated probabilities to be
     * foreign keys and their table membership.
     *
     * @param store                   where all metadata resides in
     * @param indCC                   the {@link InclusionDependency}s to be classified
     * @param uccCC                   {@link UniqueColumnCombination}s that may be referenced by foreign keys (primary keys or {@code UNIQUE} constrained columns)
     * @param columnStatisticCC       {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param textColumnStatisticCC   {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param logisticRegressionModel a logistic regression model for the estimates
     * @return one {@link Prediction} per input {@link UniqueColumnCombination}
     */
    public static Collection<InclusionDependency> estimateForeignKeys(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<UniqueColumnCombination> uccCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            VectorModel logisticRegressionModel) {

        Set<UniqueColumnCombination> uniqueColumnCombinations = new HashSet<>(uccCC.getConstraints());
        Collection<Prediction<InclusionDependency, Double>> foreignKeyProbabilities =
                estimateForeignKeyProbability(store, indCC, columnStatisticCC, textColumnStatisticCC, logisticRegressionModel);
        List<Prediction<InclusionDependency, Double>> sortedPredictions = foreignKeyProbabilities.stream()
                .filter(prediction -> prediction.getPrediction() >= 0.5d)
                .filter(prediction -> {
                    int[] referencedColumnIds = prediction.getInstance().getElement().getReferencedColumnIds().clone();
                    Arrays.sort(referencedColumnIds);
                    return uniqueColumnCombinations.contains(new UniqueColumnCombination(referencedColumnIds));
                })
                .sorted(Comparator.comparingDouble(Prediction::getPrediction))
                .collect(Collectors.toList());

        // Make sure that no column is involved in two foreign keys.
        IntSet referencingColumnIds = new IntOpenHashSet();
        Collection<InclusionDependency> estimatedForeignKeys = new ArrayList<>();
        for (Prediction<InclusionDependency, Double> prediction : sortedPredictions) {
            InclusionDependency ind = prediction.getInstance().getElement();
            boolean isUsingFreshDependentColumns = true;
            for (int columnId : ind.getDependentColumnIds()) {
                if (!(isUsingFreshDependentColumns = !referencingColumnIds.contains(columnId))) break;
            }
            if (!isUsingFreshDependentColumns) continue;
            for (int columnId : ind.getDependentColumnIds()) {
                referencingColumnIds.add(columnId);
            }
            estimatedForeignKeys.add(ind);
        }

        return estimatedForeignKeys;
    }

    /**
     * The number of features created by {@link #createFeatureVector(InclusionDependency, MetadataStore, Map, Map)}.
     */
    private static final int numFeatures = 5;

    /**
     * Create a vector representation for the given {@link InclusionDependency}, which includes
     * <ol>
     * <li>order of the referenced attributes,</li>
     * <li>contingency of the dependent attributes,</li>
     * <li>name similarity of the tables</li>
     * <li>distribution similarity, and</li>
     * <li>coverage of the foreign key.</li>
     * </ol>
     *
     * @param ind the {@link InclusionDependency}
     * @return the vector representation
     */
    private static double[] createFeatureVector(InclusionDependency ind,
                                                MetadataStore store,
                                                Map<Integer, ColumnStatistics> columnStatistics,
                                                Map<Integer, TextColumnStatistics> textColumnStatistics) {
        int[] referencedColumnIds = ind.getReferencedColumnIds();
        int[] dependentColumnIds = ind.getDependentColumnIds();

        // Count the number of misplacements.
        IdUtils idUtils = store.getIdUtils();
        int numMisplacements = 0;
        for (int i = 1; i < ind.getArity(); i++) {
            if (idUtils.getLocalColumnId(referencedColumnIds[i - 1]) > idUtils.getLocalColumnId(referencedColumnIds[i])) {
                numMisplacements++;
            }
        }

        // Count the gaps in the left-hand side.
        final int depGap = dependentColumnIds[dependentColumnIds.length - 1] - dependentColumnIds[0] - dependentColumnIds.length + 1;

        // TODO: Quantify distribution similarity. Maybe with histograms or samples.

        final int tableNameOverlap = longestSubstringLength(
                store.getTargetById(idUtils.getTableId(dependentColumnIds[0])).getName(),
                store.getTargetById(idUtils.getTableId(referencedColumnIds[0])).getName()
        );

        double columnNameOverlap = 0d;
        for (int i = 0; i < ind.getArity(); i++) {
            columnNameOverlap += longestSubstringLength(
                    store.getTargetById(dependentColumnIds[i]).getName(),
                    store.getTargetById(referencedColumnIds[i]).getName()
            );
        }
        columnNameOverlap /= ind.getArity();

        // TODO: Quantify the coverage of the foreign key.
        // We estimate the foreign key coverage simply as the average coverage of the individual attributes.
        double coverage = 0d;
        for (int i = 0; i < ind.getArity(); i++) {
            ColumnStatistics depStats = columnStatistics.get(dependentColumnIds[i]);
            ColumnStatistics refStats = columnStatistics.get(referencedColumnIds[i]);
            if (depStats == null || refStats == null) {
                coverage = 0d;
                break;
            }
            coverage += depStats.getNumDistinctValues() / (double) refStats.getNumDistinctValues();
        }
        coverage /= ind.getArity();
        if (Double.isNaN(coverage)) coverage = 0d;

        // Assemble the feature vector.
        return new double[]{numMisplacements, depGap, tableNameOverlap, columnNameOverlap, coverage};
    }

    static int longestSubstringLength(String str1, String str2) {
        if (str1.isEmpty() || str2.isEmpty()) return 0;
        if (str1.length() > str2.length()) {
            String temp = str1;
            str1 = str2;
            str2 = temp;
        }

        int[] currentRow = new int[str1.length()];
        int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
        int longestSubstringLength = 0;

        for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
            char str2Char = str2.charAt(str2Index);
            for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
                int newLength;
                if (str1.charAt(str1Index) == str2Char) {
                    newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;
                    longestSubstringLength = Math.max(newLength, longestSubstringLength);
                } else {
                    newLength = 0;
                }
                currentRow[str1Index] = newLength;
            }
            int[] temp = currentRow;
            currentRow = lastRow;
            lastRow = temp;
        }
        return longestSubstringLength;
    }
}
