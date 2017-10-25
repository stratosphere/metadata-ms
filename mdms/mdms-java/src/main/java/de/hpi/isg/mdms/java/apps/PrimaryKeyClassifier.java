package de.hpi.isg.mdms.java.apps;


import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.AppExecutionMetadata;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.TextColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This application uses simple heuristics to identify primary keys among {@link UniqueColumnCombination}s.
 *
 * @author Thorsten Papenbrock
 * @author Sebastian Kruse
 */
public class PrimaryKeyClassifier extends MdmsAppTemplate<PrimaryKeyClassifier.Parameters> {

    private DependencyPrettyPrinter prettyPrinter;

    /**
     * Statistics to use them in the classification process. Loaded before job execution.
     */
    private Int2ObjectMap<ColumnStatistics> columnStatics;

    /**
     * Statistics to use them in the classification process. Loaded before job execution.
     */
    private Int2ObjectMap<TextColumnStatistics> textColumnStatistics;

    /**
     * Runs this app programmatically.
     *
     * @param mds                on which the app should be run
     * @param uccCCId            the ID for a {@link UniqueColumnCombination} {@link ConstraintCollection} to be classified
     * @param statisticsCCId     the ID for a {@link ColumnStatistics} {@link ConstraintCollection}
     * @param textStatisticsCCId the ID for a {@link TextColumnStatistics} {@link ConstraintCollection}
     * @param resultId           a user-defined ID for the {@link ConstraintCollection} with primary keys or {@code null}
     * @return the ID of the created {@link ConstraintCollection}
     * @throws Exception
     */
    public static int fromParameters(MetadataStore mds,
                                     int uccCCId,
                                     int statisticsCCId,
                                     int textStatisticsCCId,
                                     String resultId) throws Exception {

        PrimaryKeyClassifier.Parameters parameters = new PrimaryKeyClassifier.Parameters();

        parameters.uccCollectionId = uccCCId;
        parameters.statisticsCollectionId = statisticsCCId;
        parameters.textStatisticsCollectionId = textStatisticsCCId;
        parameters.resultId = resultId;
        parameters.metadataStoreParameters.isCloseMetadataStore = false;

        PrimaryKeyClassifier app = new PrimaryKeyClassifier(parameters);
        app.metadataStore = mds;

        app.run();

        AppExecutionMetadata executionMetadata = app.getExecutionMetadata();
        if (!executionMetadata.isAppSuccess()) {
            throw new RuntimeException("The primary key classification failed.");
        }

        return (int) executionMetadata.getCustomData().get("constraintCollectionId");
    }

    public PrimaryKeyClassifier(final PrimaryKeyClassifier.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

        this.prettyPrinter = new DependencyPrettyPrinter(this.metadataStore);

        // Load the statistics.
        this.columnStatics = new Int2ObjectOpenHashMap<>();
        for (ColumnStatistics statistic : this.metadataStore
                .<ColumnStatistics>getConstraintCollection(this.parameters.statisticsCollectionId)
                .getConstraints()) {
            this.columnStatics.put(statistic.getColumnId(), statistic);
        }
        this.textColumnStatistics = new Int2ObjectOpenHashMap<>();
        for (TextColumnStatistics statistic : this.metadataStore
                .<TextColumnStatistics>getConstraintCollection(this.parameters.textStatisticsCollectionId)
                .getConstraints()) {
            this.textColumnStatistics.put(statistic.getColumnId(), statistic);
        }
    }

    @Override
    protected void executeAppLogic() throws Exception {
        final ConstraintCollection<?> uccCollection = this.metadataStore
                .getConstraintCollection(this.parameters.uccCollectionId);

        // Group the UCCs by their table.
        final IdUtils idUtils = this.metadataStore.getIdUtils();
        final Collection<List<UniqueColumnCombination>> uccGroups = uccCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof UniqueColumnCombination)
                .map(constraint -> (UniqueColumnCombination) constraint)
                .collect(Collectors.groupingBy(ucc -> {
                    final int anyColumnid = ucc.getAllTargetIds()[0];
                    return idUtils.getTableId(anyColumnid);
                })).values();

        final Stream<UniqueColumnCombination> pkStream = uccGroups.stream().map(this::determinePrimaryKey);

        if (this.parameters.isDryRun) {
            pkStream.forEach(pk -> System.out.format("Designate %s as primary key.\n",
                    this.prettyPrinter.prettyPrint(pk)));
        } else {
            final ConstraintCollection<UniqueColumnCombination> constraintCollection = this.metadataStore.createConstraintCollection(
                    this.parameters.resultId,
                    String.format("Primary keys (%s)", new Date()),
                    null,
                    UniqueColumnCombination.class,
                    uccCollection.getScope().toArray(new Target[uccCollection.getScope().size()]));
            pkStream.forEach(constraintCollection::add);
            this.metadataStore.flush();

            getLogger().info("Added a constraint collection with the ID {}.", constraintCollection.getId());
            this.executionMetadata.addCustomData("constraintCollectionId", constraintCollection.getId());
        }
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    private UniqueColumnCombination determinePrimaryKey(Collection<UniqueColumnCombination> uccs) {
        double bestScore = -1d;
        UniqueColumnCombination bestUcc = null;
        for (UniqueColumnCombination ucc : uccs) {
            double score = calculateKeyScoreOf(ucc);
            if (score > bestScore) {
                bestUcc = ucc;
                bestScore = score;
            }
        }
        return bestUcc;
    }

    /**
     * Calculates the key score of a UCC by aggregating partial key scores.
     */
    private double calculateKeyScoreOf(UniqueColumnCombination ucc) {
        if (containsNullValues(ucc)) return 0d;
        return (this.calculateKeyLengthScore(ucc) +
                this.calculateKeyPositionScore(ucc)) / 3;
    }

    /**
     * Calculate key scores based on their length. The shorter, the better.
     */
    private double calculateKeyLengthScore(UniqueColumnCombination ucc) {
        int length = ucc.getArity();
        return (length == 0) ? 0 : (1.0d / length);
    }

    /**
     * Calculate key scores based on their placement in the table. The more to the left, the better. The more contiguous,
     * the better.
     */
    private double calculateKeyPositionScore(UniqueColumnCombination ucc) {
        return (this.calculateLeftScore(ucc) + this.calculateCoherenceScore(ucc)) / 2;
    }

    /**
     * Calculates partial key score based on the position of the left-most column.
     */
    private double calculateLeftScore(UniqueColumnCombination ucc) {
        int attributesLeft = this.getMinColumnIndex(ucc);
        return (attributesLeft == 0) ? 1 : (1.0d / (attributesLeft + 1));
    }

    /**
     * Calculates a partial key score based on the "contingency" of its columns in the table.
     */
    private double calculateCoherenceScore(UniqueColumnCombination ucc) {
        int gapSize = this.getGapSum(ucc);
        return (gapSize == 0) ? 1 : (1.0d / (gapSize + 1));
    }

    private int getMinColumnIndex(UniqueColumnCombination ucc) {
        return Arrays.stream(ucc.getAllTargetIds())
                .map(this.metadataStore.getIdUtils()::getLocalColumnId)
                .min().orElseThrow(IllegalArgumentException::new);
    }

    private int getMaxColumnIndex(UniqueColumnCombination ucc) {
        return Arrays.stream(ucc.getAllTargetIds())
                .map(this.metadataStore.getIdUtils()::getLocalColumnId)
                .max().orElseThrow(IllegalArgumentException::new);
    }

    /**
     * Calculate the number of attributes between the left-most and right-most attribute of a UCC that
     * are not used in the UCC.
     */
    private int getGapSum(UniqueColumnCombination ucc) {
        final int numSpannedColumns = getMaxColumnIndex(ucc) - this.getMinColumnIndex(ucc) + 1;
        return numSpannedColumns - ucc.getArity();
    }

    /**
     * Checks whether any of the columns in the {@code ucc} contains {@code NULL} values.
     */
    private boolean containsNullValues(UniqueColumnCombination ucc) {
        return Arrays.stream(ucc.getAllTargetIds())
                .anyMatch(columnId -> {
                    final ColumnStatistics columnStatistics = this.columnStatics.get(columnId);
                    return columnStatistics != null && columnStatistics.getNumNulls() > 0;
                });
    }

    /**
     * Parameters for the {@link PrimaryKeyClassifier} app.
     */
    public static class Parameters {

        @Parameter(names = {"--statistics"},
                description = "ID of the constraint collection that contains single column statistics",
                required = true)
        public int statisticsCollectionId;

        @Parameter(names = {"--statistics"},
                description = "ID of the constraint collection that contains single text column statistics",
                required = true)
        public int textStatisticsCollectionId;

        @Parameter(names = {"--uccs"},
                description = "ID of the constraint collection that contains unique column combinations",
                required = true)
        public int uccCollectionId;

        @Parameter(names = {"--result-id"},
                description = "user-defined ID for the constraint collection with the primary keys")
        public String resultId;

        @Parameter(names = "--dry-run",
                description = "do not create a constraint collection for the foreign keys",
                required = false)
        public boolean isDryRun = false;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();
    }

    public static void main(String[] args) throws Exception {
        PrimaryKeyClassifier.Parameters parameters = new PrimaryKeyClassifier.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new PrimaryKeyClassifier(parameters).run();
    }

}
