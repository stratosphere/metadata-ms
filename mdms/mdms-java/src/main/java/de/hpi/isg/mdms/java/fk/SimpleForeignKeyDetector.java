package de.hpi.isg.mdms.java.fk;

import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.java.fk.classifiers.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This app tries to distinguish actual foreign keys among a set of inclusion dependencies. For this purpose, it takes
 * as input a {@link ConstraintCollection} of
 * {@link InclusionDependency} and derives from that {@link SimpleForeignKeyDetector.ForeignKeyCandidate}s, which it deems plausible foreign keys.
 */
public class SimpleForeignKeyDetector {

    private static final Logger logger = LoggerFactory.getLogger(SimpleForeignKeyDetector.class);

    /**
     * Propose and score {@link SimpleForeignKeyDetector.ForeignKeyCandidate}s among the {@link InclusionDependency}s from the {@code indCC}.
     *
     * @param indCC                a{@link ConstraintCollection} of {@link InclusionDependency}s
     * @param uniqueCCs            {@link ConstraintCollection}s of {@link UniqueColumnCombination}s
     * @param tupleCountCCs        {@link ConstraintCollection}s of {@link TupleCount}s
     * @param statsCCs             {@link ConstraintCollection}s of {@link ColumnStatistics}
     * @param textStatsCCs         {@link ConstraintCollection}s of {@link TextColumnStatistics}
     * @param isNeglectEmptyTables whether empty {@link Table}s should not be considered
     * @return the {@link SimpleForeignKeyDetector.ForeignKeyCandidate}s
     */
    public static List<SimpleForeignKeyDetector.ForeignKeyCandidate> detect(
            ConstraintCollection<InclusionDependency> indCC,
            Collection<ConstraintCollection<UniqueColumnCombination>> uniqueCCs,
            Collection<ConstraintCollection<TupleCount>> tupleCountCCs,
            Collection<ConstraintCollection<ColumnStatistics>> statsCCs,
            Collection<ConstraintCollection<TextColumnStatistics>> textStatsCCs,
            boolean isNeglectEmptyTables) {

        final MetadataStore metadataStore = indCC.getMetadataStore();
        final IdUtils idUtils = metadataStore.getIdUtils();

        List<UniqueColumnCombination> uccs = (uniqueCCs == null || uniqueCCs.isEmpty()) ?
                null :
                uniqueCCs.stream()
                        .flatMap(c -> c.getConstraints().stream())
                        .collect(Collectors.toList());
        List<TupleCount> tupleCounts = (tupleCountCCs == null || tupleCountCCs.isEmpty()) ?
                null :
                tupleCountCCs.stream()
                        .flatMap(c -> c.getConstraints().stream())
                        .collect(Collectors.toList());
        List<ColumnStatistics> stats = (statsCCs == null || statsCCs.isEmpty()) ?
                null :
                statsCCs.stream()
                        .flatMap(c -> c.getConstraints().stream())
                        .collect(Collectors.toList());
        List<TextColumnStatistics> textStats = (textStatsCCs == null || textStatsCCs.isEmpty()) ?
                null :
                textStatsCCs.stream()
                        .flatMap(c -> c.getConstraints().stream())
                        .collect(Collectors.toList());

        // Collect all not-null columns.
        logger.info("Detecting not-null columns...");
        IntSet nullableColumns = new IntOpenHashSet();
        if (stats != null) stats.stream()
                .filter(columnStatistics -> columnStatistics.getNumNulls() > 0)
                .forEach(columnStatistics -> nullableColumns.add(columnStatistics.getColumnId()));

        // Find PK candidates (by removing UCCs with nullable columns) and index them by their table.
        logger.info("Identifying PK candidates...");
        final Map<Integer, List<UniqueColumnCombination>> tableUccs = (uccs == null || uccs.isEmpty()) ?
                null :
                uccs.stream()
                        .filter(ucc -> Arrays.stream(ucc.getColumnIds()).noneMatch(nullableColumns::contains))
                        .collect(Collectors.groupingBy(
                                ucc -> idUtils.getTableId(ucc.getAllTargetIds()[0]),
                                Collectors.toList()));

        final IntSet nonEmptyTableIds = new IntOpenHashSet();
        if (isNeglectEmptyTables && tupleCounts != null) {
            logger.info("Loading tuple counts...");
            tupleCounts.stream()
                    .filter(tupleCount -> tupleCount.getNumTuples() > 0)
                    .forEach(tupleCount -> nonEmptyTableIds.add(tupleCount.getTableId()));
            logger.info("Found {} non-empty tables.", nonEmptyTableIds.size());
        }

        // Keep only those INDs that reference primary key (candidates).
        logger.info("Filter PK-referencing, non-empty INDs...");
        final List<InclusionDependency> relevantInds = indCC.getConstraints().stream()
                .filter(ind -> {
                    if (!isNeglectEmptyTables) return true;
                    final int depTableId = idUtils.getTableId(ind.getDependentColumnIds()[0]);
                    final int refTableId = idUtils.getTableId(ind.getReferencedColumnIds()[0]);
                    return nonEmptyTableIds.contains(depTableId) && nonEmptyTableIds.contains(refTableId);
                })
                .filter(ind -> {
                    if (tableUccs == null) return true;
                    final int[] refColumnIds = ind.getReferencedColumnIds();
                    final int tableId = idUtils.getTableId(refColumnIds[0]);
                    final List<UniqueColumnCombination> tableUccList = tableUccs.get(tableId);
                    return tableUccList != null && tableUccList.stream()
                            .anyMatch(ucc -> uccIsReferenced(ucc, ind, true));
                })
                .collect(Collectors.toList());
        logger.info("Detected {} relevant INDs from {} INDs overall.", relevantInds.size(), indCC.getConstraints().size());

        // Create classification sets for the remaining INDs.
        logger.info("Creating and running the partial classifiers...");
        final Map<UnaryForeignKeyCandidate, ClassificationSet> fkCandidateClassificationSet = relevantInds.stream()
                .flatMap(SimpleForeignKeyDetector::splitIntoUnaryForeignKeyCandidates)
                .distinct()
                .map(ClassificationSet::new)
                .collect(Collectors.toMap(
                        ClassificationSet::getForeignKeyCandidate,
                        Function.identity()
                ));
        // Set up the classifiers.
        final List<PartialForeignKeyClassifier> partialClassifiers = new LinkedList<>();
//        this.partialClassifiers.add(new CoverageClassifier(1d, 0.99d, 0.99d, dvcCollection));
        if (stats != null) {
            partialClassifiers.add(new CoverageClassifier(1d, 0.9d, 0.9d, stats));
        }
//        this.partialClassifiers.add(new CoverageClassifier(1d, 0.75d, 0.4d, dvcCollection));
//        this.partialClassifiers.add(new DependentAndReferencedClassifier(1d, 2));
//        this.partialClassifiers.add(new MultiDependentClassifier(1d, 2));
//        this.partialClassifiers.add(new MultiReferencedClassifier(1d, 2));
//        this.partialClassifiers.add(new ValueDiffClassifier(1d, statsCollection, 0.95, 0.5));
        if (textStats != null) {
            partialClassifiers.add(new ValueDiffClassifier(1d, textStats, 0.9, 0.5));
            partialClassifiers.add(new ValueDiffClassifier(Double.NaN, textStats, 0.01, 0.3));
        }
//        this.partialClassifiers.add(new ValueDiffClassifier(1d, statsCollection, 0.75, 0.5));
        double tndWeight = 1.5d / 3;
        partialClassifiers.add(new TableNameDiffClassifier(tndWeight, 3, metadataStore));
        partialClassifiers.add(new TableNameDiffClassifier(tndWeight, 5, metadataStore));
        partialClassifiers.add(new TableNameDiffClassifier(tndWeight, 7, metadataStore));
        double srtnWeight = 1.5d / 6;
        partialClassifiers.add(new ShortReferencedTableNameClassifier(srtnWeight, 5, metadataStore));
        partialClassifiers.add(new ShortReferencedTableNameClassifier(srtnWeight, 7, metadataStore));
        partialClassifiers.add(new ShortReferencedTableNameClassifier(srtnWeight, 9, metadataStore));
        partialClassifiers.add(new ShortReferencedTableNameClassifier(srtnWeight, 11, metadataStore));
        partialClassifiers.add(new ShortReferencedTableNameClassifier(srtnWeight, 13, metadataStore));
        partialClassifiers.add(new ShortReferencedTableNameClassifier(srtnWeight, 15, metadataStore));
//        this.partialClassifiers.add(new NoReferencingPKClassifier(10d, uccCollection));
        // ...

        // Run classifiers.
        partialClassifiers.forEach(classifier -> classifier.classify(fkCandidateClassificationSet.values()));

        // Calculate the score for all the inclusion dependencies.
        final List<SimpleForeignKeyDetector.ForeignKeyCandidate> indRatings = relevantInds.stream()
                .map(ind -> {
                    double indScore = splitIntoUnaryForeignKeyCandidates(ind)
                            .map(fkCandidateClassificationSet::get)
                            .mapToDouble(ClassificationSet::getOverallScore)
                            .average().orElse(0d);
                    final Map<UnaryForeignKeyCandidate, List<ClassificationSet>> reasoning =
                            splitIntoUnaryForeignKeyCandidates(ind)
                                    .map(fkCandidateClassificationSet::get)
                                    .collect(Collectors.groupingBy(ClassificationSet::getForeignKeyCandidate));
                    return new SimpleForeignKeyDetector.ForeignKeyCandidate(ind, indScore, reasoning);
                })
                .sorted((rating1, rating2) -> Double.compare(rating2.score, rating1.score))
                .collect(Collectors.toList());

        // Greedily pick the best INDs and check consistency with already picked INDs.
        logger.info("Picking the best INDs as FKs...");
        Int2ObjectMap<IntSet> tablePrimaryKeys = new Int2ObjectOpenHashMap<>();
        IntSet usedDependentColumns = new IntOpenHashSet();

        return indRatings.stream()
                .filter(indRating -> {
                    // Check that none of the dependent attributes is part of an already picked IND.
                    if (Arrays.stream(indRating.ind.getDependentColumnIds())
                            .anyMatch(usedDependentColumns::contains)) return false;

                    // The referenced attributes imply a primary key: Check that no other foreign key has been picked.
                    final int tableId = idUtils.getTableId(indRating.ind.getReferencedColumnIds()[0]);
                    final IntSet refTablePK = tablePrimaryKeys.get(tableId);
                    if (refTablePK != null &&
                            (refTablePK.size() != indRating.ind.getArity() ||
                                    !Arrays.stream(indRating.ind.getReferencedColumnIds())
                                            .allMatch(refTablePK::contains))) {
                        return false;
                    }

                    // It's settled: we accept the IND. Update the PKs and used dependent attributes accordingly.
                    Arrays.stream(indRating.ind.getDependentColumnIds())
                            .forEach(usedDependentColumns::add);
                    if (refTablePK == null) {
                        tablePrimaryKeys.put(
                                tableId,
                                new IntOpenHashSet(indRating.ind.getReferencedColumnIds()));
                    }
                    return true;
                })
                .collect(Collectors.toList());
    }

    /**
     * Tells whether an {@link InclusionDependency} references a {@link UniqueColumnCombination}.
     *
     * @param ucc                 a {@link UniqueColumnCombination}
     * @param inclusionDependency a {@link InclusionDependency}
     * @param isWithUccSupersets  if referencing a superset of the {@code ucc} is also allowed
     * @return whether the referencing relationship exists
     */
    private static boolean uccIsReferenced(UniqueColumnCombination ucc, InclusionDependency inclusionDependency,
                                           boolean isWithUccSupersets) {
        final int[] uccColumnIds = ucc.getColumnIds();
        final int[] refColumnIds = inclusionDependency.getReferencedColumnIds();

        // Conclude via the cardinalities of the UCC and IND.
        if (uccColumnIds.length > refColumnIds.length || (!isWithUccSupersets && uccColumnIds.length < refColumnIds.length)) {
            return false;
        }

        // Make a brute-force inclusion check.
        return Arrays.stream(uccColumnIds).allMatch(uccColumnId -> arrayContains(refColumnIds, uccColumnId));
    }

    private static boolean arrayContains(int[] array, int element) {
        for (int i : array) {
            if (i == element) return true;
        }
        return false;
    }

    /**
     * Splits a given {@link InclusionDependency} into {@link UnaryForeignKeyCandidate}s.
     *
     * @param ind the {@link InclusionDependency} to split
     * @return the {@link UnaryForeignKeyCandidate}s
     */
    private static Stream<UnaryForeignKeyCandidate> splitIntoUnaryForeignKeyCandidates(InclusionDependency ind) {
        List<UnaryForeignKeyCandidate> fkCandidates = new ArrayList<>(ind.getArity());
        final int[] depColumnIds = ind.getDependentColumnIds();
        final int[] refColumnIds = ind.getReferencedColumnIds();
        for (int i = 0; i < ind.getArity(); i++) {
            fkCandidates.add(new UnaryForeignKeyCandidate(depColumnIds[i], refColumnIds[i]));
        }
        return fkCandidates.stream();
    }


    /**
     * This class amends and {@link InclusionDependency} with a score.
     */
    public static class ForeignKeyCandidate {

        public final InclusionDependency ind;

        public final double score;

        public final Map<UnaryForeignKeyCandidate, List<ClassificationSet>> reasoning;

        public ForeignKeyCandidate(InclusionDependency ind, double score,
                                   Map<UnaryForeignKeyCandidate, List<ClassificationSet>> reasoning) {
            this.ind = ind;
            this.score = score;
            this.reasoning = reasoning;
        }

        public String explain(DependencyPrettyPrinter prettyPrinter) {
            StringBuilder sb = new StringBuilder();
            sb.append("Rating: ").append(String.format("%.2f", this.score)).append(", because ");
            String separator = "";
            for (Map.Entry<UnaryForeignKeyCandidate, List<ClassificationSet>> entry : reasoning.entrySet()) {
                final UnaryForeignKeyCandidate fkCandidate = entry.getKey();
                InclusionDependency ind = new InclusionDependency(
                        new int[]{fkCandidate.getDependentColumnId()},
                        new int[]{fkCandidate.getReferencedColumnId()});
                sb.append(separator).append(prettyPrinter.prettyPrint(ind)).append(": {");
                separator = "";
                for (ClassificationSet classificationSet : entry.getValue()) {
                    for (PartialForeignKeyClassifier.WeightedResult weightedResult : classificationSet.getPartialResults()) {
                        sb.append(separator).append(weightedResult);
                        separator = ", ";
                    }
                    sb.append("}");
                }
                separator = ", ";
            }

            return sb.toString();
        }

        @Override
        public String toString() {
            return String.format("score(%s)=%,5f", this.ind, this.score);
        }
    }

}