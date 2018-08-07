package de.hpi.isg.mdms.metanome;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.OmniscientResultReceiver;
import de.metanome.algorithm_integration.results.*;

import java.util.Collection;

/**
 * A {@link OmniscientResultReceiver} implementaiton that writes any result into a {@link ConstraintCollection}.
 */
public class MetacrateResultReceiver implements OmniscientResultReceiver, AutoCloseable {

    private final MetadataStore metadataStore;

    private final String descriptionPattern, userDefinedIdPrefix;

    private final Collection<Target> scope;

    private final Schema schema;

    /**
     * Creates a new instance.
     *
     * @param metadataStore      to which results should be written
     * @param schema             the {@link Schema} in which the results reside
     * @param scope              of the to be created {@link ConstraintCollection}s
     * @param descriptionPattern with one {@code %s} that should be replaced by the type of result for each
     *                           {@link ConstraintCollection}
     */
    public MetacrateResultReceiver(MetadataStore metadataStore,
                                   Schema schema,
                                   Collection<Target> scope,
                                   String descriptionPattern) {
        this(metadataStore, schema, scope, descriptionPattern, null);
    }

    /**
     * Creates a new instance.
     *
     * @param metadataStore      to which results should be written
     * @param schema             the {@link Schema} in which the results reside
     * @param scope              of the to be created {@link ConstraintCollection}s
     * @param descriptionPattern with one {@code %s} that should be replaced by the type of result for each
     *                           {@link ConstraintCollection}
     */
    public MetacrateResultReceiver(MetadataStore metadataStore,
                                   Schema schema,
                                   Collection<Target> scope,
                                   String descriptionPattern,
                                   String userDefinedIdPrefix) {
        this.metadataStore = metadataStore;
        this.schema = schema;
        this.scope = scope;
        this.descriptionPattern = descriptionPattern;
        this.userDefinedIdPrefix = userDefinedIdPrefix;
    }

    private StatisticsResultReceiver statisticsResultReceiver;

    @Override
    synchronized public void receiveResult(BasicStatistic statistic) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.statisticsResultReceiver == null) this.statisticsResultReceiver = new StatisticsResultReceiver(
                this.metadataStore,
                this.schema,
                this.scope,
                this.userDefinedIdPrefix,
                String.format(this.descriptionPattern, "statistics")
        );
        this.statisticsResultReceiver.receiveResult(statistic);
    }

    @Override
    public Boolean acceptedResult(BasicStatistic result) {
        return true;
    }

    @Override
    synchronized public void receiveResult(ConditionalUniqueColumnCombination conditionalUniqueColumnCombination) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        throw new CouldNotReceiveResultException("Result type is not supported.");
    }

    @Override
    public Boolean acceptedResult(ConditionalUniqueColumnCombination result) {
        return false;
    }

    @Override
    public void receiveResult(MatchingDependency matchingDependency) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        throw new CouldNotReceiveResultException("Result type is not supported.");
    }

    @Override
    public Boolean acceptedResult(MatchingDependency matchingDependency) {
        return false;
    }

    @Override
    public void receiveResult(ConditionalFunctionalDependency conditionalFunctionalDependency) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        throw new CouldNotReceiveResultException("Result type is not supported.");
    }

    @Override
    public Boolean acceptedResult(ConditionalFunctionalDependency conditionalFunctionalDependency) {
        return false;
    }

    @Override
    public void receiveResult(DenialConstraint denialConstraint) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        throw new CouldNotReceiveResultException("Result type is not supported.");
    }

    @Override
    public Boolean acceptedResult(DenialConstraint denialConstraint) {
        return false;
    }

    private DependencyResultReceiver<de.hpi.isg.mdms.domain.constraints.FunctionalDependency> fdResultReceiver;

    @Override
    synchronized public void receiveResult(FunctionalDependency functionalDependency) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.fdResultReceiver == null) this.fdResultReceiver = new DependencyResultReceiver<>(
                this.metadataStore,
                this.schema,
                this.scope,
                de.hpi.isg.mdms.domain.constraints.FunctionalDependency.class,
                String.format(this.descriptionPattern, "FDs"),
                this.userDefinedIdPrefix == null ? null : this.userDefinedIdPrefix + "fds"
        );
        this.fdResultReceiver.receiveResult(functionalDependency);
    }

    @Override
    public Boolean acceptedResult(FunctionalDependency result) {
        return true;
    }

    private DependencyResultReceiver<de.hpi.isg.mdms.domain.constraints.InclusionDependency> indResultReceiver;

    @Override
    synchronized public void receiveResult(InclusionDependency inclusionDependency) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.indResultReceiver == null) this.indResultReceiver = new DependencyResultReceiver<>(
                this.metadataStore,
                this.schema,
                this.scope,
                de.hpi.isg.mdms.domain.constraints.InclusionDependency.class,
                String.format(this.descriptionPattern, "INDs"),
                this.userDefinedIdPrefix == null ? null : this.userDefinedIdPrefix + "inds"
        );
        this.indResultReceiver.receiveResult(inclusionDependency);
    }

    @Override
    public Boolean acceptedResult(InclusionDependency result) {
        return true;
    }

    @Override
    public void receiveResult(MultivaluedDependency multivaluedDependency) throws CouldNotReceiveResultException, ColumnNameMismatchException {

    }

    @Override
    public Boolean acceptedResult(MultivaluedDependency result) {
        return false;
    }

    @Override
    public void receiveResult(OrderDependency orderDependency) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        throw new CouldNotReceiveResultException("Result type is not supported.");
    }

    @Override
    public Boolean acceptedResult(OrderDependency result) {
        return false;
    }

    private DependencyResultReceiver<de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination> uccResultReceiver;

    @Override
    synchronized public void receiveResult(UniqueColumnCombination uniqueColumnCombination) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.uccResultReceiver == null) this.uccResultReceiver = new DependencyResultReceiver<>(
                this.metadataStore,
                this.schema,
                this.scope,
                de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.class,
                String.format(this.descriptionPattern, "UCCs"),
                this.userDefinedIdPrefix == null ? null : this.userDefinedIdPrefix + "uccs"
        );
        this.uccResultReceiver.receiveResult(uniqueColumnCombination);
    }

    @Override
    public Boolean acceptedResult(UniqueColumnCombination result) {
        return true;
    }

    /**
     * Provide the {@link MetadataStore} to which this instance writes.
     *
     * @return the {@link MetadataStore}
     */
    public MetadataStore getMetadataStore() {
        return this.metadataStore;
    }

    @Override
    synchronized public void close() throws Exception {
        if (this.statisticsResultReceiver != null) this.statisticsResultReceiver.close();
        if (this.fdResultReceiver != null) this.fdResultReceiver.close();
        if (this.uccResultReceiver != null) this.uccResultReceiver.close();
        if (this.indResultReceiver != null) this.indResultReceiver.close();
    }
}
