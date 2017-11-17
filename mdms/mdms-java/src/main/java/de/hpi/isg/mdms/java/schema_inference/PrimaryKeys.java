package de.hpi.isg.mdms.java.schema_inference;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.TextColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.java.ml.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class models primary key detection as a machine learning problem.
 */
public class PrimaryKeys {

    /**
     * Create a training set for primary key classification.
     *
     * @param store                 where all metadata resides in
     * @param primaryKeyCC          known primary keys
     * @param uccCC                 all {@link UniqueColumnCombination}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @return the training dataset consisting of {@link Observation}s
     */
    public static Collection<Observation<UniqueColumnCombination>> createTrainingSet(
            MetadataStore store,
            ConstraintCollection<UniqueColumnCombination> primaryKeyCC,
            ConstraintCollection<UniqueColumnCombination> uccCC,
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
        Set<IntList> primaryKeys = primaryKeyCC.getConstraints().stream()
                .map(pk -> new IntArrayList(pk.getColumnIds()))
                .collect(Collectors.toSet());

        return uccCC.getConstraints().stream()
                .map(ucc -> new Observation<>(
                        ucc,
                        createFeatureVector(ucc, store.getIdUtils(), columnStatistics, textColumnStatistics),
                        primaryKeys.contains(new IntArrayList(ucc.getColumnIds())) ? 1d : 0d)
                )
                .collect(Collectors.toList());
    }

    /**
     * Create a dataset that is ready for primary key classification.
     *
     * @param store                 where all metadata resides in
     * @param uccCC                 all {@link UniqueColumnCombination}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @return the estimation dataset consisting of {@link Instance}s
     */
    public static Collection<Instance<UniqueColumnCombination>> createEstimationSet(
            MetadataStore store,
            ConstraintCollection<UniqueColumnCombination> uccCC,
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

        return uccCC.getConstraints().stream()
                .map(ucc -> new Instance<>(
                        ucc,
                        createFeatureVector(ucc, store.getIdUtils(), columnStatistics, textColumnStatistics)
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
    public static Collection<Observation<UniqueColumnCombination>> oversample(
            Collection<Observation<UniqueColumnCombination>> instances) {
        List<Observation<UniqueColumnCombination>> positiveInstances = new ArrayList<>();
        List<Observation<UniqueColumnCombination>> negativeInstances = new ArrayList<>();
        for (Observation<UniqueColumnCombination> instance : instances) {
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
     * Train a logistic regression model for the classification of primary keys.
     *
     * @param store                 where all metadata resides in
     * @param primaryKeyCC          known primary keys
     * @param uccCC                 all {@link UniqueColumnCombination}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @return the {@link VectorModel} for the trained logistic regression model
     */
    public static VectorModel trainLogisticRegressionModel(
            MetadataStore store,
            ConstraintCollection<UniqueColumnCombination> primaryKeyCC,
            ConstraintCollection<UniqueColumnCombination> uccCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC) {
        Collection<Observation<UniqueColumnCombination>> trainingSet =
                createTrainingSet(store, primaryKeyCC, uccCC, columnStatisticCC, textColumnStatisticCC);
        trainingSet = oversample(trainingSet);
        return LogisticRegression.train(trainingSet, features.length - 1, 1d, 5, 0.0001);
    }

    /**
     * Estimate the probability of {@link UniqueColumnCombination}s to be primary keys.
     *
     * @param store                   where all metadata resides in
     * @param uccCC                   all {@link UniqueColumnCombination}s
     * @param columnStatisticCC       {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param textColumnStatisticCC   {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param logisticRegressionModel a logistic regression model for the estimates
     * @return one {@link Prediction} per input {@link UniqueColumnCombination}
     */
    public static Collection<Prediction<UniqueColumnCombination, Double>> estimatePrimaryKeyProbability(
            MetadataStore store,
            ConstraintCollection<UniqueColumnCombination> uccCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            VectorModel logisticRegressionModel) {
        Collection<Instance<UniqueColumnCombination>> estimationSet = createEstimationSet(store, uccCC, columnStatisticCC, textColumnStatisticCC);
        return LogisticRegression.estimateAll(estimationSet, logisticRegressionModel);
    }

    /**
     * Select potential primary keys among {@link UniqueColumnCombination}s based on their estimated probabilities to be
     * primary keys and their table membership.
     *
     * @param store                   where all metadata resides in
     * @param uccCC                   all {@link UniqueColumnCombination}s
     * @param columnStatisticCC       {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param textColumnStatisticCC   {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns
     * @param logisticRegressionModel a logistic regression model for the estimates
     * @return one {@link Prediction} per input {@link UniqueColumnCombination}
     */
    public static Collection<UniqueColumnCombination> estimatePrimaryKeys(
            MetadataStore store,
            ConstraintCollection<UniqueColumnCombination> uccCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            VectorModel logisticRegressionModel) {
        Collection<Prediction<UniqueColumnCombination, Double>> primaryKeyProbabilities =
                estimatePrimaryKeyProbability(store, uccCC, columnStatisticCC, textColumnStatisticCC, logisticRegressionModel);
        Int2ObjectMap<Prediction<UniqueColumnCombination, Double>> bestCandidates = new Int2ObjectOpenHashMap<>();
        for (Prediction<UniqueColumnCombination, Double> classifiedInstance : primaryKeyProbabilities) {
            int tableId = store.getIdUtils().getTableId(classifiedInstance.getInstance().getElement().getColumnIds()[0]);
            bestCandidates.merge(tableId, classifiedInstance, (a, b) -> a.getPrediction() > b.getPrediction() ? a : b);
        }

        return bestCandidates.values().stream()
                .map(r -> r.getInstance().getElement())
                .collect(Collectors.toList());
    }

    public static final String[] features = {
            "Fill status",
            "Arity",
            "Maximum value length",
            "Columns to the left",
            "Gaps between attributes",
            "Offset"
    };

    /**
     * Combine several data profiles to form a vector representation of a {@link UniqueColumnCombination}.
     *
     * @param ucc                  that should be represented as a vector
     * @param idUtils              to translate column to table IDs
     * @param columnStatistics     {@link ColumnStatistics} for the {@link UniqueColumnCombination} columns; indexed by the column IDs
     * @param textColumnStatistics {@link TextColumnStatistics} for the {@link UniqueColumnCombination} columns; indexed by the column IDs
     * @return the vector representation of the {@link UniqueColumnCombination}
     */
    private static double[] createFeatureVector(UniqueColumnCombination ucc,
                                                IdUtils idUtils,
                                                Map<Integer, ColumnStatistics> columnStatistics,
                                                Map<Integer, TextColumnStatistics> textColumnStatistics) {
        final int arity = ucc.getArity();
        int maximumValueLength = 0;
        int[] columnIds = ucc.getColumnIds();
        double fillStatus = 1d;
        for (int columnId : columnIds) {
            TextColumnStatistics tcs = textColumnStatistics.get(columnId);
            if (tcs != null && tcs.getLongestValue() != null) maximumValueLength += tcs.getLongestValue().length();
            ColumnStatistics cs = columnStatistics.get(columnId);
            if (cs != null) fillStatus *= cs.getFillStatus();
        }
        final int leftGap = idUtils.getLocalColumnId(columnIds[0]) - idUtils.minColumnNumber;
        final int midGap = columnIds[columnIds.length - 1] - columnIds[0] - columnIds.length + 1;

        return new double[]{fillStatus, arity, maximumValueLength, leftGap, midGap};
    }

}
