package de.hpi.isg.mdms.java.schema_inference;

import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.java.ml.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.doubles.Double2IntAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class models foreign key detection as a machine learning problem.
 */
public class ForeignKeys {

    private static final Logger logger = LoggerFactory.getLogger(ForeignKeys.class);

    /**
     * Create a training set for foreign key classification.
     *
     * @param store                 where all metadata resides in
     * @param foreignKeyCC          known foreign keys
     * @param indCC                 all {@link InclusionDependency}s
     * @param columnStatisticCC     {@link ColumnStatistics} for the {@link InclusionDependency} columns
     * @param textColumnStatisticCC {@link TextColumnStatistics} for the {@link InclusionDependency} columns
     * @param tableSampleCC         {@link TableSample} for the {@link InclusionDependency} tables
     * @return the training dataset consisting of {@link Observation}s
     */
    public static Collection<Observation<InclusionDependency>> createTrainingSet(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> foreignKeyCC,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            ConstraintCollection<TableSample> tableSampleCC) {
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
        Map<Integer, TableSample> tableSamples = tableSampleCC.getConstraints().stream()
                .collect(Collectors.toMap(
                        TableSample::getTableId,
                        Function.identity()
                ));
        Set<InclusionDependency> foreignKeys = new HashSet<>(foreignKeyCC.getConstraints());

        return indCC.getConstraints().stream()
                .map(ind -> new Observation<>(
                        ind,
                        createFeatureVector(ind, store, columnStatistics, textColumnStatistics, tableSamples),
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
     * @param tableSampleCC         {@link TableSample} for the {@link InclusionDependency} tables
     * @return the estimation dataset consisting of {@link Instance}s
     */
    public static Collection<Instance<InclusionDependency>> createEstimationSet(
            MetadataStore store,
            ConstraintCollection<InclusionDependency> indCC,
            ConstraintCollection<ColumnStatistics> columnStatisticCC,
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            ConstraintCollection<TableSample> tableSampleCC) {
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
        Map<Integer, TableSample> tableSamples = tableSampleCC.getConstraints().stream()
                .collect(Collectors.toMap(
                        TableSample::getTableId,
                        Function.identity()
                ));

        return indCC.getConstraints().stream()
                .map(ind -> new Instance<>(
                        ind,
                        createFeatureVector(ind, store, columnStatistics, textColumnStatistics, tableSamples)
                ))
                .collect(Collectors.toList());
    }

    /**
     * Balance a training set (as per {@link #createTrainingSet(MetadataStore, ConstraintCollection, ConstraintCollection, ConstraintCollection, ConstraintCollection, ConstraintCollection)}
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
            ConstraintCollection<TextColumnStatistics> textColumnStatisticCC,
            ConstraintCollection<TableSample> tableSampleCC) {
        Collection<Observation<InclusionDependency>> trainingSet =
                createTrainingSet(store, foreignKeyCC, indCC, columnStatisticCC, textColumnStatisticCC, tableSampleCC);
        trainingSet = oversample(trainingSet);
        return LogisticRegression.train(trainingSet, features.length - 1, 1d, 5, 0.0001);
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
            ConstraintCollection<TableSample> tableSampleCC,
            VectorModel logisticRegressionModel) {
        Collection<Instance<InclusionDependency>> estimationSet = createEstimationSet(store, indCC, columnStatisticCC, textColumnStatisticCC, tableSampleCC);
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
            ConstraintCollection<TableSample> tableSampleCC,
            VectorModel logisticRegressionModel) {

        Set<UniqueColumnCombination> uniqueColumnCombinations = new HashSet<>(uccCC.getConstraints());
        Collection<Prediction<InclusionDependency, Double>> foreignKeyProbabilities =
                estimateForeignKeyProbability(store, indCC, columnStatisticCC, textColumnStatisticCC, tableSampleCC, logisticRegressionModel);
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
     * The number of features created by {@link #createFeatureVector(InclusionDependency, MetadataStore, Map, Map, Map)}.
     */
    public static final String[] features = new String[]{
            "Unorderedness of referenced columns",
            "Gaps between dependent columns",
            "Table name similarity (excl. equality)",
            "Column name similarity",
            "Distance of value distributions",
            "Value coverage of the referenced columns",
            "Offset"
    };

    /**
     * Create a vector representation for the given {@link InclusionDependency}, which includes
     * <ol>
     * <li>order of the referenced attributes,</li>
     * <li>contingency of the dependent attributes,</li>
     * <li>name similarity of the tables,</li>
     * <li>name similarity of the columns,</li>
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
                                                Map<Integer, TextColumnStatistics> textColumnStatistics,
                                                Map<Integer, TableSample> tableSamples) {
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

        // Calculate the average distance between the dependent and referenced value distributions.
        double distributionDistance = 0d;
        for (int i = 0; i < ind.getArity(); i++) {
            distributionDistance += estimateDistributionDistance(
                    dependentColumnIds[i],
                    tableSamples.get(idUtils.getTableId(dependentColumnIds[i])),
                    referencedColumnIds[i],
                    tableSamples.get(idUtils.getTableId(referencedColumnIds[i])),
                    idUtils
            );
        }
        distributionDistance /= ind.getArity();

        // Deprecated estimation for the proximity of value distributions.
//        double probabilityThatRefIsSmallerThanDep = 0d;
//        for (int i = 0; i < ind.getArity(); i++) {
//            probabilityThatRefIsSmallerThanDep += testWilcoxonRankSum(
//                    dependentColumnIds[i],
//                    tableSamples.get(idUtils.getTableId(dependentColumnIds[i])),
//                    referencedColumnIds[i],
//                    tableSamples.get(idUtils.getTableId(referencedColumnIds[i])),
//                    idUtils
//            );
//        }
//        probabilityThatRefIsSmallerThanDep /= ind.getArity();
//        final double distributionalSimilarity;
//        if (Double.isNaN(probabilityThatRefIsSmallerThanDep)) {
//            // If we could not do this test, then we are conservative.
//            distributionalSimilarity = 0d;
//        } else {
//            // Try to linearize the estimated probability: 0.5 indicates distributional similarity, while 0 and 1 don't.
//            distributionalSimilarity = 1 - Math.abs(2 * probabilityThatRefIsSmallerThanDep - 1);
//        }

        String depTableName = store.getTargetById(idUtils.getTableId(dependentColumnIds[0])).getName();
        String refTableName = store.getTargetById(idUtils.getTableId(referencedColumnIds[0])).getName();
        final int tableNameOverlap = depTableName.equals(refTableName) ? 0 : longestSubstringLength(depTableName, refTableName);

        double columnNameOverlap = 0d;
        for (int i = 0; i < ind.getArity(); i++) {
            columnNameOverlap += longestSubstringLength(
                    store.getTargetById(dependentColumnIds[i]).getName(),
                    store.getTargetById(referencedColumnIds[i]).getName()
            );
        }
        columnNameOverlap /= ind.getArity();

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
        return new double[]{numMisplacements, depGap, tableNameOverlap, columnNameOverlap, distributionDistance, coverage};
    }

    /**
     * Determine the longest common substring of two strings.
     *
     * @return the length of the longest common substring
     */
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

    /**
     * Inspired by the Wilcoxon Rank Sum test, we calculate the probability for a pair of randomly drawn values
     * from {@code depColumn} and {@code refColumn} that the {@code refColumn} value is smaller. The actual used order
     * is dependent on the data.
     *
     * @param depColumnId    ID of the dependent column
     * @param depTableSample {@link TableSample} for the dependent column; nullable
     * @param refColumnId    ID of the referenced column
     * @param refTableSample {@link TableSample} for the referenced column; nullable
     * @param idUtils        can manipulate IDs of the given columns and tables
     * @return said probability; or {@code Double#NaN} if no such probability could be determined (e.g., because
     * we lack sampling data)
     */
    @SuppressWarnings("unused")
    static double testWilcoxonRankSum(int depColumnId,
                                      TableSample depTableSample,
                                      int refColumnId,
                                      TableSample refTableSample,
                                      IdUtils idUtils) {
        // Extract the column values.
        ArrayList<String> depColumn = extractColumnSample(depColumnId, depTableSample, idUtils);
        ArrayList<String> refColumn = extractColumnSample(refColumnId, refTableSample, idUtils);

        // Make sure that we have all the data we need.
        final int minValues = 20;
        if (depColumn == null || depColumn.size() < minValues || refColumn == null || refColumn.size() < minValues)
            return Double.NaN;

        // Try to test the rank sum based on numeric order, otherwise fall back to lexicographical order.
        double result = testNumericWilcoxonRankSum(depColumn, refColumn);
        if (!Double.isNaN(result)) return result;
        return testTextWilcoxonRankSum(depColumn, refColumn);
    }

    /**
     * Extracts a column vector from a given {@link TableSample}.
     *
     * @param columnId    the ID of the column whose values should be extracted
     * @param tableSample the sample of the table that contains the column; nullable
     * @param idUtils     helps to convert column to table IDs
     * @return the extract column vector without {@code null} values; or {@code null} if no vector could be extracted
     */
    private static ArrayList<String> extractColumnSample(int columnId, TableSample tableSample, IdUtils idUtils) {
        if (tableSample == null) return null;
        assert tableSample.getTableId() == idUtils.getTableId(columnId);
        int columnIndex = idUtils.getLocalColumnId(columnId) - idUtils.minColumnNumber;
        ArrayList<String> column = new ArrayList<>(tableSample.getTuples().length);
        for (String[] row : tableSample.getTuples()) {
            if (columnIndex < row.length && row[columnIndex] != null) {
                column.add(row[columnIndex]);
            }
        }
        return column;
    }

    /**
     * Inspired by the Wilcoxon Rank Sum test, we calculate the probability for a pair of randomly drawn numbers
     * from {@code depColumn} and {@code refColumn} that the {@code refColumn} value is smaller.
     *
     * @param depColumn dependent values; should not be empty
     * @param refColumn referenced values; should not be empty
     * @return said probability; or {@code Double#NaN} if no such probability could be determined (e.g., because
     * the parameters do not exclusively contain numeric data)
     */
    private static double testNumericWilcoxonRankSum(ArrayList<String> depColumn, ArrayList<String> refColumn) {
        // Try to convert the two columns to numbers.
        ArrayList<Double> depNumbers = parseColumn(depColumn);
        ArrayList<Double> refNumbers = parseColumn(refColumn);
        if (depNumbers == null || refNumbers == null) return Double.NaN;

        // Turn the referenced numbers into a histogram.
        Double2IntAVLTreeMap refNumberHistogram = new Double2IntAVLTreeMap();
        refNumberHistogram.defaultReturnValue(0);
        for (double refNumber : refNumbers) {
            int count = refNumberHistogram.get(refNumber);
            refNumberHistogram.put(refNumber, count + 1);
        }

        // Sort the dependent values.
        depNumbers.sort(Double::compare);

        // Co-iterated the dependent and referenced values to count the value pairs where the referenced value is
        // small than the dependent value (count ties as 0.5).
        double fractionSmallerRefs = 0d;
        int currentSmallerRefs = 0;
        Iterator<Double> depIterator = depNumbers.iterator();
        ObjectBidirectionalIterator<Double2IntMap.Entry> refIterator = refNumberHistogram.double2IntEntrySet().iterator();
        assert refIterator.hasNext();
        Double2IntMap.Entry currentRefEntry = refIterator.next();

        while (depIterator.hasNext()) {
            // Get the next dependent value.
            double depValue = depIterator.next();

            // Move the referenced iterator forward as much as possible.
            while (currentRefEntry != null && currentRefEntry.getDoubleKey() < depValue) {
                currentSmallerRefs += currentRefEntry.getIntValue();
                if (refIterator.hasNext()) {
                    currentRefEntry = refIterator.next();
                } else {
                    currentRefEntry = null;
                    // TODO: We can simply calculate the remainder of this method without actually enacting it.
                }
            }

            fractionSmallerRefs += currentSmallerRefs;
            if (currentRefEntry != null && currentRefEntry.getDoubleKey() == depValue) {
                fractionSmallerRefs += currentRefEntry.getIntValue() / 2d;
            }
        }

        // Normalize the pair count by the number of pairs.
        fractionSmallerRefs /= depNumbers.size() * refNumbers.size();
        return fractionSmallerRefs;
    }

    /**
     * Inspired by the Wilcoxon Rank Sum test, we calculate the probability for a pair of randomly drawn strings
     * from {@code depColumn} and {@code refColumn} that the {@code refColumn} value is lexicographically smaller.
     *
     * @param depColumn dependent values; should not be empty
     * @param refColumn referenced values; should not be empty
     * @return said probability
     */
    static double testTextWilcoxonRankSum(ArrayList<String> depColumn, ArrayList<String> refColumn) {
        // Turn the referenced values into a histogram.
        Object2IntAVLTreeMap<String> refValueHistogram = new Object2IntAVLTreeMap<>();
        refValueHistogram.defaultReturnValue(0);
        for (String refValue : refColumn) {
            int count = refValueHistogram.getInt(refValue);
            refValueHistogram.put(refValue, count + 1);
        }

        // Sort the dependent values.
        depColumn.sort(String::compareTo);

        // Co-iterated the dependent and referenced values to count the value pairs where the referenced value is
        // small than the dependent value (count ties as 0.5).
        double fractionSmallerRefs = 0d;
        int currentSmallerRefs = 0;
        Iterator<String> depIterator = depColumn.iterator();
        ObjectBidirectionalIterator<Object2IntMap.Entry<String>> refIterator = refValueHistogram.object2IntEntrySet().iterator();
        assert refIterator.hasNext();
        Object2IntMap.Entry<String> currentRefEntry = refIterator.next();

        while (depIterator.hasNext()) {
            // Get the next dependent value.
            String depValue = depIterator.next();

            // Move the referenced iterator forward as much as possible.
            while (currentRefEntry != null && currentRefEntry.getKey().compareTo(depValue) < 0) {
                currentSmallerRefs += currentRefEntry.getIntValue();
                if (refIterator.hasNext()) {
                    currentRefEntry = refIterator.next();
                } else {
                    currentRefEntry = null;
                    // TODO: We can simply calculate the remainder of this method without actually enacting it.
                }
            }

            fractionSmallerRefs += currentSmallerRefs;
            if (currentRefEntry != null && currentRefEntry.getKey().equals(depValue)) {
                fractionSmallerRefs += currentRefEntry.getIntValue() / 2d;
            }
        }

        // Normalize the pair count by the number of pairs.
        fractionSmallerRefs /= depColumn.size() * refColumn.size();
        return fractionSmallerRefs;
    }

    /**
     * Calculates the distributional similarity between two columns using histograms and the Earth Mover's Distance.
     *
     * @return a similarity value between {@code 0} and {@code 1}
     */
    static double estimateDistributionDistance(int depColumnId,
                                               TableSample depTableSample,
                                               int refColumnId,
                                               TableSample refTableSample,
                                               IdUtils idUtils) {
        ArrayList<String> refColumn = extractColumnSample(refColumnId, refTableSample, idUtils);
        ArrayList<String> depColumn = extractColumnSample(depColumnId, depTableSample, idUtils);
        if (refColumn.isEmpty() || depColumn.isEmpty()) {
            return 0d; // Conservative: If we can't establish a match, it's not a match.
        }

        List<double[]> histograms = computeHistograms(refColumn, depColumn);
        double[] refHistogram = histograms.get(0);
        double[] depHistogram = histograms.get(1);

        // Calculate how much "earth" has to be moved from/to every bucket.
        Object2DoubleMap<IntList> sourceBuckets = new Object2DoubleOpenHashMap<>();
        Object2DoubleMap<IntList> targetBuckets = new Object2DoubleOpenHashMap<>();
        for (int i = 0; i < depHistogram.length; i++) {
            double diff = refHistogram[i] - depHistogram[i];
            if (diff < 0) sourceBuckets.put(new IntArrayList(new int[]{i}), -diff);
            else if (diff > 0) targetBuckets.put(new IntArrayList(new int[]{i}), diff);
        }
        IntList normalizers = new IntArrayList(new int[]{depHistogram.length});

        // Calculate the distance matrix and source and target node weights.
        double[] sourceNodes = new double[sourceBuckets.size()];
        double[] targetNodes = new double[targetBuckets.size()];
        List<Object2DoubleMap.Entry<IntList>> fixedTargetBuckets = new ArrayList<>(targetBuckets.object2DoubleEntrySet());
        double[][] distance = new double[sourceBuckets.size()][targetBuckets.size()];
        int sourceIndex = 0;
        for (Object2DoubleMap.Entry<IntList> sourceBucketEntry : sourceBuckets.object2DoubleEntrySet()) {
            sourceNodes[sourceIndex] = sourceBucketEntry.getDoubleValue();
            int targetIndex = 0;
            IntList sourceBucket = sourceBucketEntry.getKey();
            for (Object2DoubleMap.Entry<IntList> targetBucketEntry : fixedTargetBuckets) {
                targetNodes[targetIndex] = targetBucketEntry.getDoubleValue();
                IntList targetBucket = targetBucketEntry.getKey();
                assert !sourceBucket.equals(targetBucket);
                distance[sourceIndex][targetIndex] = sourceBucket.equals(targetBucket) ?
                        Double.POSITIVE_INFINITY :
                        normalizedManhattanDistance(sourceBucket, targetBucket, normalizers);
                targetIndex++;
            }
            sourceIndex++;
        }

        return calculateEarthMoverDistance(sourceNodes, targetNodes, distance);
    }

    /**
     * Create histograms for the two given lists. The domain order assumed is number ordering or, if that is not
     * applicable, lexicographical ordering. The histograms use the same buckets.
     *
     * @param refColumn the first list
     * @param depColumn the second list
     * @return (normalized) histograms for {@code refColumn} and {@code depColumn}
     */
    private static List<double[]> computeHistograms(ArrayList<String> refColumn, ArrayList<String> depColumn) {
        ArrayList<Double> depNumbers = parseColumn(depColumn);
        ArrayList<Double> refNumbers = parseColumn(refColumn);
        return (depNumbers == null || refNumbers == null) ?
                computeHistograms(refColumn, depColumn, String::compareTo) :
                computeHistograms(refNumbers, depNumbers, Double::compare);
    }

    /**
     * Create histograms for the two given lists. The histograms use the same buckets.
     *
     * @param refColumn  the first list
     * @param depColumn  the second list
     * @param comparator defines the ordering of values
     * @return (normalized) histograms for {@code refColumn} and {@code depColumn}
     */
    private static <T> List<double[]> computeHistograms(ArrayList<T> refColumn,
                                                        ArrayList<T> depColumn,
                                                        Comparator<T> comparator) {
        // Create the quantile histogram for the referenced column.
        refColumn.sort(comparator);

        // Count and sort the distinct values in the column.
        List<T> distinctRefValues = new ArrayList<>(new HashSet<>(refColumn));
        distinctRefValues.sort(comparator);
        int numBuckets = Math.min(10, distinctRefValues.size() + 1);
        double avgDistinctValuesPerBucket = distinctRefValues.size() / (double) numBuckets;

        // Initialize the bucket borders.
        List<T> bucketBorders = new ArrayList<>(numBuckets - 1);
        for (int i = 1; i < numBuckets; i++) {
            bucketBorders.add(distinctRefValues.get((int) (i * avgDistinctValuesPerBucket)));
        }

        // Create the histograms for the dependent and referenced values.
        return Arrays.asList(
                computeHistogram(refColumn, bucketBorders, comparator),
                computeHistogram(depColumn, bucketBorders, comparator)
        );
    }

    /**
     * Calculate the normalized Manhattan distance.
     *
     * @param a           coordinates
     * @param b           coordinates
     * @param normalizers normalization nominators for each dimension
     * @return the normalized Manhattan distance between {@code a} and {@code b}
     */
    private static double normalizedManhattanDistance(IntList a, IntList b, IntList normalizers) {
        double d = 0;
        for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            d += Math.abs(a.getInt(i) - b.getInt(i)) / (double) normalizers.getInt(i);
        }
        return d;
    }

    /**
     * Compute the histogram for a {@code column}.
     *
     * @param column        values to be represented in the histogram
     * @param bucketBorders define the border values between two adjacent buckets
     * @param comparator    defines the value order
     * @return a {@code double} array with a field for each bucket; border values are split among the adjacent buckets;
     * the histogram is normalized (i.e., the buckets sum up to {@code 1})
     */
    private static <T> double[] computeHistogram(ArrayList<T> column, List<T> bucketBorders, Comparator<T> comparator) {
        double[] histogram = new double[bucketBorders.size() + 1];
        for (T value : column) {
            int pos = Collections.binarySearch(bucketBorders, value, comparator);
            if (pos >= 0) {
                // The value is a border value, so distribute the value over the two adjacent buckets.
                histogram[pos] += 0.5;
                histogram[pos + 1] += 0.5;
            } else {
                pos = -pos - 1;
                // The value is smaller than the matched border value, so we add the value to the preceeding bucket.
                histogram[pos] += 1;
            }
        }
        // Eventually, normalize the histogram.
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] /= column.size();
        }
        return histogram;
    }

    /**
     * Parse a {@link String} column to {@link Double} values.
     *
     * @param column the {@link String} column
     * @return the {@code column} translated to {@link Double} values or {@code null} if the {@code column} cannot be completely parsed
     */
    private static ArrayList<Double> parseColumn(Collection<String> column) {
        try {
            ArrayList<Double> doubles = new ArrayList<>(column.size());
            for (String value : column) {
                doubles.add(Double.parseDouble(value));
            }
            return doubles;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Calculates the Earth Mover's Distance between source and target nodes. The source and target distribution
     * should have the same probability mass of {@code 1}. Also, the distance measure should be normalized.
     *
     * @param sourceNodes a histogram of source distribution
     * @param targetNodes a histogram of the target distribution
     * @param distance    a distance matrix between the source and target nodes/buckets
     * @return the Earth Mover's Distance (which should be between {@code 0} and {@code 1} if the input parameters
     * are normalized
     */
    static double calculateEarthMoverDistance(double[] sourceNodes, double[] targetNodes, double[][] distance) {
        logEmdState("Initial problem:", sourceNodes, targetNodes, distance);

        double sourceSum = Arrays.stream(sourceNodes).sum();
        double targetSum = Arrays.stream(targetNodes).sum();
        assert approximatelyEquals(sourceSum, targetSum);

        // Step 1: Copy the distance matrix and select best edges.
        double[][] distanceCopy = new double[distance.length][];
        for (int i = 0; i < distance.length; i++) {
            distanceCopy[i] = distance[i].clone();
        }
        for (int source = 0; source < sourceNodes.length; source++) {
            double min = Double.POSITIVE_INFINITY;
            for (int target = 0; target < targetNodes.length; target++) {
                min = Math.min(min, distanceCopy[source][target]);
            }
            for (int target = 0; target < targetNodes.length; target++) {
                distanceCopy[source][target] -= min;
            }
        }
        for (int target = 0; target < targetNodes.length; target++) {
            double min = Double.POSITIVE_INFINITY;
            for (int source = 0; source < sourceNodes.length; source++) {
                min = Math.min(min, distanceCopy[source][target]);
            }
            for (int source = 0; source < sourceNodes.length; source++) {
                distanceCopy[source][target] -= min;
            }
        }

        // Stores the capacity/demand of the source/target nodes.
        double[] sourceCapacity = sourceNodes.clone();
        double[] targetCapacity = targetNodes.clone();
        // Indexes the edges.
        Map<FlowGraphNode, List<FlowGraphNode>> flowGraphEdges = new HashMap<>();
        for (int source = 0; source < sourceNodes.length; source++) {
            FlowGraphNode sourceNode = new FlowGraphNode(source, true);
            for (int target = 0; target < targetNodes.length; target++) {
                FlowGraphNode targetNode = new FlowGraphNode(target, false);
                if (distanceCopy[source][target] <= 0d) {
                    flowGraphEdges.computeIfAbsent(sourceNode, k -> new ArrayList<>()).add(targetNode);
                    flowGraphEdges.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(sourceNode);
                    logger.debug("Adding {} -> {}.", sourceNode, targetNode);
                }
            }
        }
        // Stores the flow happening on each edge.
        double[][] flow = new double[sourceCapacity.length][targetCapacity.length];

        // Loop to enlarge the graph if it does not yet provide a flow.
        while (true) {
            logEmdState("Current distances:", sourceCapacity, targetCapacity, distanceCopy);

            // Loop to find a new shortest path.
            while (true) {
                // Look for shortest paths in our flow graph.
                PriorityQueue<FlowGraphPath> queue = new PriorityQueue<>(FlowGraphPath.distanceComparator);
                for (int i = 0; i < sourceCapacity.length; i++) {
                    if (!isZero(sourceCapacity[i])) {
                        queue.add(new FlowGraphPath(new FlowGraphNode(i, true), null, 0));
                    }
                }
                Set<FlowGraphNode> visitedNodes = new HashSet<>();
                Deque<FlowGraphNode> shortestPath = null;
                while (!queue.isEmpty()) {
                    FlowGraphPath path = queue.poll();
                    if (!visitedNodes.add(path.node)) continue;
                    // Check whether this path substantiates a complete flow.
                    if (!path.node.isSourceNode && targetCapacity[path.node.index] > 0d) {
                        shortestPath = new LinkedList<>();
                        do {
                            shortestPath.addFirst(path.node);
                            path = path.predecessor;
                        } while (path != null);
                        break;
                    }
                    // If not, check which edges we can follow.
                    for (FlowGraphNode nextNode : flowGraphEdges.getOrDefault(path.node, Collections.emptyList())) {
                        if (!visitedNodes.contains(nextNode)
                                && (!nextNode.isSourceNode || flow[nextNode.index][path.node.index] > 0d)) {
                            queue.offer(new FlowGraphPath(nextNode, path, path.distance + 1));
                        }
                    }
                }

                if (shortestPath != null) {
                    // Augment the flow graph.
                    // First, figure out the maximum capacity on the flow.
                    double pathFlow = sourceCapacity[shortestPath.getFirst().index];
                    FlowGraphNode lastNode = null;
                    for (FlowGraphNode currentNode : shortestPath) {
                        if (lastNode != null) {
                            if (lastNode.isSourceNode) {
                                // We only restrict the flow towards target nodes, when they are not "filled".
                                if (!isZero(targetCapacity[currentNode.index]))
                                    pathFlow = Math.min(pathFlow, targetCapacity[currentNode.index]);
                            } else {
                                pathFlow = Math.min(pathFlow, flow[currentNode.index][lastNode.index]);
                            }
                        }
                        lastNode = currentNode;
                    }
                    // Update the node capacities and flows.
                    lastNode = null;
                    for (FlowGraphNode currentNode : shortestPath) {
                        if (lastNode != null) {
                            if (lastNode.isSourceNode) {
                                flow[lastNode.index][currentNode.index] += pathFlow;
                            } else {
                                flow[currentNode.index][lastNode.index] -= pathFlow;
                            }
                        }
                        lastNode = currentNode;
                    }
                    sourceCapacity[shortestPath.getFirst().index] -= pathFlow;
                    targetCapacity[shortestPath.getLast().index] -= pathFlow;

                    logger.debug("Augmenting flow {} with {}.", shortestPath, pathFlow);
                    logEmdState("Capacities and flows:", sourceCapacity, targetCapacity, flow);

                } else {
                    // If there is no shortest path any more, we have constructed the maximum flow.
                    break;
                }
            }

            // Check if we have found an exhaustive flow.
            BitSet nonEmptySources = new BitSet(sourceNodes.length);
            BitSet labeledTargets = new BitSet(targetNodes.length);
            for (int i = 0; i < sourceNodes.length; i++) {
                if (!isZero(sourceCapacity[i])) {
                    nonEmptySources.set(i);
                    for (FlowGraphNode targetNode : flowGraphEdges.getOrDefault(new FlowGraphNode(i, true), Collections.emptyList())) {
                        labeledTargets.set(targetNode.index);
                    }
                }
            }

            if (nonEmptySources.isEmpty()) {
                // If we are complete, calculate the overall distance.
                double emd = 0d;
                for (int source = 0; source < sourceNodes.length; source++) {
                    for (int target = 0; target < targetNodes.length; target++) {
                        emd += distance[source][target] * flow[source][target];
                    }
                }
                return emd;
            } else {
                if (labeledTargets.cardinality() == targetNodes.length) {
                    throw new IllegalStateException();
                }

                // Find the minimum relevant edge.
                double minDistance = Double.POSITIVE_INFINITY;
                for (int source = nonEmptySources.nextSetBit(0); source != -1; source = nonEmptySources.nextSetBit(source + 1)) {
                    for (int target = labeledTargets.nextClearBit(0); target < targetNodes.length; target = labeledTargets.nextClearBit(target + 1)) {
                        if (distanceCopy[source][target] > 0)
                            minDistance = Math.min(minDistance, distanceCopy[source][target]);
                    }
                }
                if (minDistance == 0d || minDistance == Double.POSITIVE_INFINITY) {
                    throw new IllegalStateException("The calculation of earth mover's distance appears to have failed.");
                }

                // Subtract the minimum costs.
                for (int source = nonEmptySources.nextSetBit(0); source != -1; source = nonEmptySources.nextSetBit(source + 1)) {
                    FlowGraphNode sourceNode = new FlowGraphNode(source, true);
                    for (int target = labeledTargets.nextClearBit(0); target < targetNodes.length; target = labeledTargets.nextClearBit(target + 1)) {
                        if (distanceCopy[source][target] > 0 && (distanceCopy[source][target] -= minDistance) == 0d) {
                            FlowGraphNode targetNode = new FlowGraphNode(target, false);
                            flowGraphEdges.computeIfAbsent(sourceNode, k -> new ArrayList<>()).add(targetNode);
                            flowGraphEdges.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(sourceNode);
                            logger.debug("Adding {} -> {}.", sourceNode, targetNode);
                        }
                    }
                }
                // Add the minimum costs.
                for (int source = nonEmptySources.nextClearBit(0); source < sourceNodes.length; source = nonEmptySources.nextClearBit(source + 1)) {
                    for (int target = labeledTargets.nextSetBit(0); target != -1; target = labeledTargets.nextSetBit(target + 1)) {
                        if (distanceCopy[source][target] > 0d) {
                            distanceCopy[source][target] += minDistance;
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings("unused")
    private static void logEmdState(String title, double[] source, double[] target, double[][] transitions) {
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append(title).append("\n").append(String.format("%8s", ""));
            for (double v : target) {
                sb.append(String.format(" % 8.5f", v));
            }
            sb.append('\n');
            for (int i = 0; i < source.length; i++) {
                sb.append(String.format("% 8.5f", source[i]));
                for (double v : transitions[i]) {
                    sb.append(String.format(" % 8.5f", v));
                }
                sb.append('\n');
            }
            logger.debug(sb.toString());
        }
    }

    private static boolean approximatelyEquals(double a, double b) {
        final double epsilon = 0.000001d;
        return !Double.isNaN(a) && !Double.isNaN(b) && a - epsilon < b && b < a + epsilon;
    }

    private static boolean isZero(double d) {
        final double epsilon = 0.000001d;
        return !Double.isNaN(d) && -epsilon < d && d < epsilon;
    }

    private static class FlowGraphNode {

        final int index;

        final boolean isSourceNode;

        public FlowGraphNode(int index, boolean isSourceNode) {
            this.index = index;
            this.isSourceNode = isSourceNode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final FlowGraphNode that = (FlowGraphNode) o;

            if (index != that.index) return false;
            return isSourceNode == that.isSourceNode;
        }

        @Override
        public int hashCode() {
            int result = index;
            result = 31 * result + (isSourceNode ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return String.format("[%s %d]", this.isSourceNode ? "src" : "trg", this.index);
        }
    }

    private static class FlowGraphPath {

        final int distance;

        final FlowGraphPath predecessor;

        final FlowGraphNode node;

        FlowGraphPath(FlowGraphNode node, FlowGraphPath predecessor, int distance) {
            this.distance = distance;
            this.predecessor = predecessor;
            this.node = node;
        }

        static final Comparator<FlowGraphPath> distanceComparator = Comparator.comparingDouble(fgp -> fgp.distance);

    }
}