package de.hpi.isg.mdms.tools.metanome;

import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.BasicStatisticsResultReceiver;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.BasicStatistic;
import de.metanome.algorithm_integration.results.basic_statistic_values.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Writes all received results to a specified {@link MetadataStore}.
 * Receives the Metanome results and extracts the relevant information.
 *
 * @author Susanne Buelow
 * @author Sebastian Kruse
 */
public class StatisticsResultReceiver implements AutoCloseable, BasicStatisticsResultReceiver {

    /**
     * Pattern to extract JSON keys for the top k frequent values.
     */
    private static final Pattern TOP_K_FREQUENT_VALUES_PATTERN = Pattern.compile("Frequency Of Top (\\d+) Frequent Items");

    /**
     * Pattern to generate JSON key for the top k frequent values.
     */
    private static final String TOP_K_FREQUENT_VALUES_FORMAT = "Top %d frequent items";

    private static final String FREQUENCY_TOP_K_ITEMS_FORMAT = "Frequency Of Top %d Frequent Items";

    private ConstraintCollection<ColumnStatistics> constraintCollectionColumnStatistics;

    private ConstraintCollection<TypeConstraint> constraintCollectionTypeConstraints;

    private ConstraintCollection<NumberColumnStatistics> constraintCollectionNumberColumnStatistics;

    private ConstraintCollection<TextColumnStatistics> constraintCollectionTextColumnStatistics;

    private final Target[] scope;

    private final MetadataStore metadataStore;

    private final String resultDescription;

    private final IdentifierResolver identifierResolver;

    public StatisticsResultReceiver(MetadataStore metadataStore,
                                    Schema schema,
                                    Collection<Target> scope,
                                    String resultDescription) {
        this.identifierResolver = new IdentifierResolver(metadataStore, schema);
        this.metadataStore = metadataStore;
        this.resultDescription = resultDescription;
        this.scope = scope.toArray(scope.toArray(new Target[scope.size()]));
    }

    @Override
    public void receiveResult(BasicStatistic basicStatistic) throws CouldNotReceiveResultException {
        // Resolve the column.
        ColumnCombination columnCombination = basicStatistic.getColumnCombination();
        if (columnCombination.getColumnIdentifiers().size() != 1) {
            throw new CouldNotReceiveResultException(
                    String.format("Can only receive statistics with exactly one column (violated by %s)", basicStatistic)
            );
        }
        ColumnIdentifier columnIdentifier = columnCombination.getColumnIdentifiers().iterator().next();
        Column column = this.identifierResolver.resolveColumn(columnIdentifier);
        Map<String, BasicStatisticValue> statistics = basicStatistic.getStatisticMap();

        this.handleTypeConstraint(statistics, column);
        this.handleColumnStatistics(statistics, column);
        this.handleTextColumnStatistics(statistics, column);
        this.handleNumberColumnStatistics(statistics, column);
    }

    /**
     * Extract a {@link TypeConstraint}.
     */
    private void handleTypeConstraint(Map<String, BasicStatisticValue> statistics, Column column) {
        // Handle the type constraint.
        if (statistics.containsKey("Data Type")) {
            TypeConstraint.buildAndAddToCollection(
                    new SingleTargetReference(column.getId()),
                    this.getConstraintCollectionTypeConstraints(),
                    statistics.get("Data Type").getValue().toString()
            );
        }
    }

    /**
     * Extracts {@link ColumnStatistics}, which apply to all columns.
     */
    private void handleColumnStatistics(Map<String, BasicStatisticValue> statistics, Column column) {
        // Handle the general column statistics.
        boolean isColumnStatisticsEmpty = true;
        ColumnStatistics columnStatistics = new ColumnStatistics(column.getId());
        if (statistics.containsKey("Nulls")) {
            columnStatistics.setNumNulls(((BasicStatisticValueLong) statistics.get("Nulls")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Percentage of Nulls")) {
            columnStatistics.setFillStatus(1d - ((BasicStatisticValueLong) statistics.get("Percentage of Nulls")).getValue() / 100d);
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Number of Distinct Values")) {
            columnStatistics.setNumDistinctValues(((BasicStatisticValueInteger) statistics.get("Number of Distinct Values")).getValue());
            isColumnStatisticsEmpty = false;
        }

        // Find the most frequent values.
        final OptionalInt maxK = statistics.keySet().stream()
                .flatMapToInt((key) -> {
                    final Matcher matcher = TOP_K_FREQUENT_VALUES_PATTERN.matcher(key);
                    if (matcher.matches()) {
                        int k = Integer.parseInt(matcher.group(1));
                        if (statistics.keySet().contains(String.format(FREQUENCY_TOP_K_ITEMS_FORMAT, k))) {
                            return IntStream.of(k);
                        }
                    }
                    return IntStream.empty();
                })
                .max();

        if (maxK.isPresent()) {
            String itemsKey = String.format(TOP_K_FREQUENT_VALUES_FORMAT, maxK.getAsInt());
            String frequencyKey = String.format(FREQUENCY_TOP_K_ITEMS_FORMAT, maxK.getAsInt());
            List<String> values = ((BasicStatisticValueStringList) statistics.get(itemsKey)).getValue();
            List<Integer> frequencies = ((BasicStatisticValueIntegerList) statistics.get(frequencyKey)).getValue();
            List<ColumnStatistics.ValueOccurrence> valueOccurrences = new ArrayList<>(values.size());
            for (int i = 0; i < Math.min(values.size(), frequencies.size()); i++) {
                valueOccurrences.add(new ColumnStatistics.ValueOccurrence(values.get(i), frequencies.get(i)));
            }
            columnStatistics.setTopKFrequentValues(valueOccurrences);
            isColumnStatisticsEmpty = false;
        }

        if (!isColumnStatisticsEmpty) this.getConstraintCollectionColumnStatistics().add(columnStatistics);
    }

    /**
     * Extracts {@link TextColumnStatistics}.
     */
    private void handleTextColumnStatistics(Map<String, BasicStatisticValue> statistics, Column column) {
        // Handle the general column statistics.
        boolean isColumnStatisticsEmpty = true;
        TextColumnStatistics columnStatistics = new TextColumnStatistics(column.getId());
        if (statistics.containsKey("Min String")) {
            columnStatistics.setMinValue(((BasicStatisticValueString) statistics.get("Min String")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Max String")) {
            columnStatistics.setMaxValue(((BasicStatisticValueString) statistics.get("Max String")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Shortest String")) {
            columnStatistics.setShortestValue(((BasicStatisticValueString) statistics.get("Shortest String")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Longest String")) {
            columnStatistics.setLongestValue(((BasicStatisticValueString) statistics.get("Longest String")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Symantic Data Type")) {
            // What can we expect if this is always a long? Either 0 or 1?
            columnStatistics.setSubtype(((BasicStatisticValueString) statistics.get("Symantic Data Type")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (!isColumnStatisticsEmpty) this.getConstraintCollectionTextColumnStatistics().add(columnStatistics);
    }

    /**
     * Extracts {@link NumberColumnStatistics}.
     */
    private void handleNumberColumnStatistics(Map<String, BasicStatisticValue> statistics, Column column) {
        // Handle the general column statistics.
        boolean isColumnStatisticsEmpty = true;
        NumberColumnStatistics columnStatistics = new NumberColumnStatistics(column.getId());
        if (statistics.containsKey("Min")) {
            columnStatistics.setMinValue(((BasicStatisticValueDouble) statistics.get("Min")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Max")) {
            columnStatistics.setMaxValue(((BasicStatisticValueDouble) statistics.get("Max")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (statistics.containsKey("Avg.")) {
            columnStatistics.setAverage(((BasicStatisticValueDouble) statistics.get("Avg.")).getValue());
            isColumnStatisticsEmpty = false;
        }
        if (!isColumnStatisticsEmpty) this.getConstraintCollectionNumberColumnStatistics().add(columnStatistics);
    }

    @Override
    public Boolean acceptedResult(BasicStatistic result) {
        return true;
    }

    public ConstraintCollection<ColumnStatistics> getConstraintCollectionColumnStatistics() {
        if (this.constraintCollectionColumnStatistics == null) {
            this.constraintCollectionColumnStatistics = this.metadataStore.createConstraintCollection(
                    this.resultDescription, ColumnStatistics.class, this.scope
            );
        }
        return this.constraintCollectionColumnStatistics;
    }

    public ConstraintCollection<TypeConstraint> getConstraintCollectionTypeConstraints() {
        if (this.constraintCollectionTypeConstraints == null) {
            this.constraintCollectionTypeConstraints = this.metadataStore.createConstraintCollection(
                    this.resultDescription, TypeConstraint.class, this.scope
            );
        }
        return this.constraintCollectionTypeConstraints;
    }

    public ConstraintCollection<NumberColumnStatistics> getConstraintCollectionNumberColumnStatistics() {
        if (this.constraintCollectionNumberColumnStatistics == null) {
            this.constraintCollectionNumberColumnStatistics = this.metadataStore.createConstraintCollection(
                    this.resultDescription, NumberColumnStatistics.class, this.scope
            );
        }
        return this.constraintCollectionNumberColumnStatistics;
    }

    public ConstraintCollection<TextColumnStatistics> getConstraintCollectionTextColumnStatistics() {
        if (this.constraintCollectionTextColumnStatistics == null) {
            this.constraintCollectionTextColumnStatistics = this.metadataStore.createConstraintCollection(
                    this.resultDescription, TextColumnStatistics.class, this.scope
            );
        }
        return this.constraintCollectionTextColumnStatistics;
    }

    @Override
    public void close() throws IOException {
        try {
            this.metadataStore.flush();
        } catch (Exception e) {
            throw new IOException("Could not flush the metadata store.", e);
        }
    }

}
