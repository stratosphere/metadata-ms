package de.hpi.isg.mdms.java.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.TupleCount;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.domain.util.SQLiteConstraintUtils;
import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.java.fk.classifiers.*;
import de.hpi.isg.mdms.java.fk.feature.*;
import de.hpi.isg.mdms.java.fk.ml.classifier.AbstractClassifier;
import de.hpi.isg.mdms.java.fk.ml.classifier.NaiveBayes;
import de.hpi.isg.mdms.java.fk.ml.evaluation.ClassifierEvaluation;
import de.hpi.isg.mdms.java.fk.ml.evaluation.FMeasureEvaluation;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This app tries to distinguish actual foreign keys among a set of inclusion dependencies. For this purpose, it takes
 * as input a {@link ConstraintCollection} of
 * {@link InclusionDependency} and derives from that a new one that only contains
 * the classified foreign keys.
 */
public class ForeignKeyClassifier extends MdmsAppTemplate<ForeignKeyClassifier.Parameters> {

    private final List<PartialForeignKeyClassifier> partialClassifiers = new LinkedList<>();

    private List<Feature> features = new ArrayList<>();

    public ForeignKeyClassifier(final ForeignKeyClassifier.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }


    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

        SQLiteConstraintUtils.registerStandardConstraints(
                (SQLiteInterface) ((RDBMSMetadataStore) this.metadataStore).getSQLInterface());
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Load all relevant constraint collections.
        getLogger().info("Loading INDs...");
        final ConstraintCollection<? extends Constraint> indCollection = this.metadataStore.getConstraintCollection(this.parameters.indCollectionId);
        getLogger().info("Loading DVCs...");
        final ConstraintCollection<? extends Constraint> dvcCollection = this.metadataStore.getConstraintCollection(this.parameters.dvcCollectionId);
        getLogger().info("Loading UCCs...");
        final ConstraintCollection<? extends Constraint> uccCollection = this.metadataStore.getConstraintCollection(this.parameters.uccCollectionId);
        getLogger().info("Loading statistics...");
        final ConstraintCollection<? extends Constraint> statsCollection = this.metadataStore.getConstraintCollection(this.parameters.statisticsCollectionId);

        // Collect all not-null columns.
        getLogger().info("Detecting not-null columns...");
        IntSet nullableColumns = new IntOpenHashSet();
        statsCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof ColumnStatistics)
                .map(constraint -> (ColumnStatistics) constraint)
                .filter(columnStatistics -> columnStatistics.getNumNulls() > 0)
                .forEach(columnStatistics -> nullableColumns.add(columnStatistics.getTargetReference().getTargetId()));

        // Find PK candidates (by removing UCCs with nullable columns) and index them by their table.
        getLogger().info("Identifying PK candidates...");
        final IdUtils idUtils = this.metadataStore.getIdUtils();
        final Map<Integer, List<UniqueColumnCombination>> tableUccs = uccCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof UniqueColumnCombination)
                .map(constraint -> (UniqueColumnCombination) constraint)
                .filter(ucc -> ucc.getTargetReference().getAllTargetIds().stream().noneMatch(nullableColumns::contains))
                .collect(Collectors.groupingBy(
                        ucc -> idUtils.getTableId(ucc.getTargetReference().getAllTargetIds().iterator().nextInt()),
                        Collectors.toList()));

        final IntSet nonEmptyTableIds = new IntOpenHashSet();
        if (this.parameters.isNeglectEmptyTables) {
            getLogger().info("Loading tuple counts...");
            statsCollection.getConstraints().stream()
                    .filter(constraint -> constraint instanceof TupleCount)
                    .map(constraint -> (TupleCount) constraint)
                    .filter(tupleCount -> tupleCount.getNumTuples() > 0)
                    .forEach(tupleCount -> nonEmptyTableIds.add(tupleCount.getTargetReference().getTargetId()));
            getLogger().info("Found {} non-empty tables.", nonEmptyTableIds.size());
        }

        // Keep only those INDs that reference primary key (candidates).
        getLogger().info("Filter PK-referencing, non-empty INDs...");
        final List<InclusionDependency> relevantInds = indCollection.getConstraints().stream()
                .map(constraint -> (InclusionDependency) constraint)
                .filter(ind -> {
                    if (!this.parameters.isNeglectEmptyTables) return true;
                    final int depTableId = idUtils.getTableId(ind.getTargetReference().getDependentColumns()[0]);
                    final int refTableId = idUtils.getTableId(ind.getTargetReference().getReferencedColumns()[0]);
                    return nonEmptyTableIds.contains(depTableId) && nonEmptyTableIds.contains(refTableId);
                })
                .filter(ind -> {
                    final int[] refColumnIds = ind.getTargetReference().getReferencedColumns();
                    final int tableId = idUtils.getTableId(refColumnIds[0]);
                    final List<UniqueColumnCombination> uccs = tableUccs.get(tableId);
                    return uccs != null && uccs.stream()
                            .anyMatch(ucc -> uccIsReferenced(ucc, ind, this.parameters.isWithUccSupersets));
                })
                .collect(Collectors.toList());
        getLogger().info("Detected {} relevant INDs from {} INDs overall.", relevantInds.size(), indCollection.getConstraints().size());

        // Create classification sets for the remaining INDs.
        getLogger().info("Creating and running the partial classifiers...");
        final Map<UnaryForeignKeyCandidate, ClassificationSet> fkCandidateClassificationSet = relevantInds.stream()
                .flatMap(this::splitIntoUnaryForeignKeyCandidates)
                .distinct()
                .map(ClassificationSet::new)
                .collect(Collectors.toMap(
                        ClassificationSet::getForeignKeyCandidate,
                        Function.identity()
                ));

        List<Instance> instances = relevantInds.stream()
                .flatMap(this::splitIntoUnaryForeignKeyCandidates)
                .distinct()
                .map(Instance::new)
                .collect(Collectors.toList());

        this.features.add(new CoverageFeature(statsCollection));
        this.features.add(new DependentAndReferencedFeature());
        this.features.add(new DistinctDependentValuesFeature(statsCollection));
        this.features.add(new MultiDependentFeature());
        this.features.add(new MultiReferencedFeature());

        Dataset dataset = new Dataset(instances, features);
        dataset.buildDatasetStatistics();
        dataset.buildFeatureValueDistribution();

        AbstractClassifier classifier = new NaiveBayes();
        classifier.setTrainingset(dataset);
        classifier.setTestset(dataset);
        classifier.train();
        classifier.predict();

        // Calculate the score for all the inclusion dependencies.
        final List<InclusionDependencyRating> indRatings = relevantInds.stream()
                .map(ind -> {
                    double indScore = splitIntoUnaryForeignKeyCandidates(ind)
                            .map(fkCandidateClassificationSet::get)
                            .mapToDouble(ClassificationSet::getOverallScore)
                            .average().orElse(0d);
                    final Map<UnaryForeignKeyCandidate, List<ClassificationSet>> reasoning =
                            splitIntoUnaryForeignKeyCandidates(ind)
                                    .map(fkCandidateClassificationSet::get)
                                    .collect(Collectors.groupingBy(ClassificationSet::getForeignKeyCandidate));
                    return new InclusionDependencyRating(ind, indScore, reasoning);
                })
                .filter(rating -> rating.score >= this.parameters.minFkScore)
                .sorted((rating1, rating2) -> Double.compare(rating2.score, rating1.score))
                .collect(Collectors.toList());

        // Greedily pick the best INDs and check consistency with already picked INDs.
        getLogger().info("Picking the best INDs as FKs...");
        Int2ObjectMap<IntSet> tablePrimaryKeys = new Int2ObjectOpenHashMap<>();
        IntSet usedDependentColumns = new IntOpenHashSet();

        final List<InclusionDependencyRating> foreignKeyRatings = indRatings.stream()
                .filter(indRating -> {
                    // Check that none of the dependent attributes is part of an already picked IND.
                    if (Arrays.stream(indRating.ind.getTargetReference().getDependentColumns())
                            .anyMatch(usedDependentColumns::contains)) return false;

                    // The referenced attributes imply a primary key: Check that no other foreign key has been picked.
                    final int tableId = idUtils.getTableId(indRating.ind.getTargetReference().getReferencedColumns()[0]);
                    final IntSet refTablePK = tablePrimaryKeys.get(tableId);
                    if (refTablePK != null &&
                            (refTablePK.size() != indRating.ind.getArity() ||
                                    !Arrays.stream(indRating.ind.getTargetReference().getReferencedColumns())
                                            .allMatch(refTablePK::contains))) {
                        return false;
                    }

                    // It's settled: we accept the IND. Update the PKs and used dependent attributes accordingly.
                    Arrays.stream(indRating.ind.getTargetReference().getDependentColumns())
                            .forEach(usedDependentColumns::add);
                    if (refTablePK == null) {
                        tablePrimaryKeys.put(
                                tableId,
                                new IntOpenHashSet(indRating.ind.getTargetReference().getReferencedColumns()));
                    }
                    return true;
                })
                .collect(Collectors.toList());

        if (this.parameters.isDryRun) {
            DependencyPrettyPrinter prettyPrinter = new DependencyPrettyPrinter(this.metadataStore);
            for (InclusionDependencyRating foreignKeyRating : foreignKeyRatings) {
                System.out.format("Chosen FK %s with a rating of %.2f.\n",
                        prettyPrinter.prettyPrint(foreignKeyRating.ind),
                        foreignKeyRating.score);
            }
        } else {
            final ConstraintCollection<TestConstraint> constraintCollection = this.metadataStore.createConstraintCollection(
                    String.format("Foreign keys (%s)", new Date()), statisticsCollectionId.class,
                    indCollection.getScope().toArray(new Target[indCollection.getScope().size()]));
            foreignKeyRatings.stream()
                    .map(fkRating -> fkRating.ind)
                    .forEach(constraintCollection::add);
            this.metadataStore.flush();
        }

        if (this.parameters.evaluationFiles.size() == 2) {
            throw new RuntimeException("Not implemented: not settled upon a source for FK gold standards.");
//            final String tableDefPath = this.parameters.evaluationFiles.get(0);
//            final String fkDefPath = this.parameters.evaluationFiles.get(1);
//            final Map<String, List<String>> tableDefinitions = MusicBrainzParser.readTableDefinitions(tableDefPath);
//            final Object2IntMap<String> columnIdDictionary = MusicBrainzParser.createColumnIdDictionary(
//                    this.metadataStore,
//                    this.metadataStore.getSchemaByName(this.parameters.schemaName),
//                    tableDefinitions);
//            final Set<InclusionDependency> goldInds =
//                    new HashSet<>(MusicBrainzParser.readGoldINDs(fkDefPath, columnIdDictionary));
//
//            if (this.parameters.isNeglectEmptyTables) {
//                // Remove INDs that pertain to empty columns.
//                final int numOriginalGoldInds = goldInds.size();
//                goldInds.removeIf(ind -> {
//                    final int depTableId = idUtils.getTableId(ind.getTargetReference().getDependentColumns()[0]);
//                    final int refTableId = idUtils.getTableId(ind.getTargetReference().getReferencedColumns()[0]);
//                    return !nonEmptyTableIds.contains(depTableId) || !nonEmptyTableIds.contains(refTableId);
//                });
//                getLogger().info("Reduced number of gold standard INDs from {} to {}.", numOriginalGoldInds, goldInds.size());
//            }
//
//            int numTruePositives = 0;
//            DependencyPrettyPrinter prettyPrinter = new DependencyPrettyPrinter(this.metadataStore);
//            for (InclusionDependencyRating foreignKeyRating : foreignKeyRatings) {
//                final InclusionDependency ind = foreignKeyRating.ind;
//                if (goldInds.contains(ind)) {
//                    System.out.format("Correct FK: %s (%s)\n",
//                            prettyPrinter.prettyPrint(ind), foreignKeyRating.explain(prettyPrinter));
//                    numTruePositives++;
//                } else {
//                    System.out.format("Wrong FK:   %s (%s)\n",
//                            prettyPrinter.prettyPrint(ind), foreignKeyRating.explain(prettyPrinter));
//                }
//            }
//
//            System.out.format("Gold INDs:      %d\n", goldInds.size());
//            System.out.format("Guessed INDs:   %d\n", foreignKeyRatings.size());
//            System.out.format("True positives: %d\n", numTruePositives);
//            System.out.format("Recall:         %.2f%%\n", numTruePositives * 100d / goldInds.size());
//            System.out.format("Precision:      %.2f%%\n", numTruePositives * 100d / foreignKeyRatings.size());


        }

        this.metadataStore.close();

    }

    /**
     * Tells whether an {@link InclusionDependency} references a {@link UniqueColumnCombination}.
     *
     * @param ucc                 a {@link UniqueColumnCombination}
     * @param inclusionDependency a {@link InclusionDependency}
     * @param isWithUccSupersets  if referencing a superset of the {@code ucc} is also allowed
     * @return whether the referencing relationship exists
     */
    private boolean uccIsReferenced(UniqueColumnCombination ucc, InclusionDependency inclusionDependency,
                                    boolean isWithUccSupersets) {
        final IntCollection uccColumnIds = ucc.getTargetReference().getAllTargetIds();
        final int[] refColumnIds = inclusionDependency.getTargetReference().getReferencedColumns();

        // Conclude via the cardinalities of the UCC and IND.
        if (uccColumnIds.size() > refColumnIds.length || (!isWithUccSupersets && uccColumnIds.size() < refColumnIds.length)) {
            return false;
        }

        // Make a brute-force inclusion check.
        return uccColumnIds.stream().allMatch(uccColumnId -> arrayContains(refColumnIds, uccColumnId));
    }

    private boolean arrayContains(int[] array, int element) {
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
    private Stream<UnaryForeignKeyCandidate> splitIntoUnaryForeignKeyCandidates(InclusionDependency ind) {
        List<UnaryForeignKeyCandidate> fkCandidates = new ArrayList<>(ind.getArity());
        final int[] depColumnIds = ind.getTargetReference().getDependentColumns();
        final int[] refColumnIds = ind.getTargetReference().getReferencedColumns();
        for (int i = 0; i < ind.getArity(); i++) {
            fkCandidates.add(new UnaryForeignKeyCandidate(depColumnIds[i], refColumnIds[i]));
        }
        return fkCandidates.stream();
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    /**
     * This class amends and {@link InclusionDependency} with a score.
     */
    private static class InclusionDependencyRating {

        private final InclusionDependency ind;

        private final double score;

        private final Map<UnaryForeignKeyCandidate, List<ClassificationSet>> reasoning;

        public InclusionDependencyRating(InclusionDependency ind, double score,
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
                InclusionDependency ind = new InclusionDependency(new InclusionDependency.Reference(
                        new int[]{fkCandidate.getDependentColumnId()},
                        new int[]{fkCandidate.getReferencedColumnId()}));
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
    }

    /**
     * Parameters for the {@link ForeignKeyClassifier} app.
     */
    public static class Parameters {

        @Parameter(names = {"--inds"},
                description = "ID of the constraint collection that contains the input INDs",
                required = true)
        public int indCollectionId;

        @Parameter(names = {"--distinct-values"},
                description = "ID of the constraint collection that contains the distinct value counts",
                required = false)
        public int dvcCollectionId;

        @Parameter(names = {"--statistics"},
                description = "ID of the constraint collection that contains single column statistics",
                required = true)
        public int statisticsCollectionId;

        @Parameter(names = {"--uccs"},
                description = "ID of the constraint collection that contains unique column combinations",
                required = true)
        public int uccCollectionId;

        @Parameter(names = {"--with-ucc-supersets"},
                description = "supersets of minimal UCCs are not considered PK candidates",
                required = false)
        public boolean isWithUccSupersets;

        @Parameter(names = {"--min-fk-score"},
                description = "minimum score for a foreign key candidate to be considered a foreign key",
                required = false)
        public double minFkScore = 0.1d;

        @Parameter(names = "--dry-run",
                description = "do not create a constraint collection for the foreign keys",
                required = false)
        public boolean isDryRun = false;

        @Parameter(names = "--evaluation-files",
                description = "table definition file and FK definition file",
                arity = 2,
                required = false)
        public List<String> evaluationFiles = new ArrayList<>(2);

        @Parameter(names = "--schema-name",
                description = "schema name for evaluation purposes",
                required = false)
        public String schemaName = null;

        @Parameter(names = {"--no-empty-tables"},
                description = "do not consider empty tables in the calculation of precision and recall",
                required = false)
        public boolean isNeglectEmptyTables = false;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();
    }

    public static void main(String[] args) throws Exception {
        ForeignKeyClassifier.Parameters parameters = new ForeignKeyClassifier.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new ForeignKeyClassifier(parameters).run();
    }
}
