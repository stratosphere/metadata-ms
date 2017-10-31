package de.hpi.isg.mdms.metanome;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.ReferenceUtils;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Writes all received results to a specified {@link MetadataStore}.
 * Receives the Metanome results and extracts the relevant information.
 *
 * @author Susanne Buelow
 * @author Sebastian Kruse
 */
public class DependencyResultReceiver<T> implements AutoCloseable,
        UniqueColumnCombinationResultReceiver, FunctionalDependencyResultReceiver, InclusionDependencyResultReceiver {

    private final ConstraintCollection<T> constraintCollection;

    private final IdentifierResolver identifierResolver;

    public DependencyResultReceiver(MetadataStore metadatastore,
                                    Schema schema,
                                    Collection<Target> scope,
                                    Class<T> constraintClass,
                                    String resultDescription,
                                    String userDefinedId) {
        this.identifierResolver = new IdentifierResolver(metadatastore, schema);
        this.constraintCollection = metadatastore.createConstraintCollection(
                userDefinedId, resultDescription, null, constraintClass, scope.toArray(scope.toArray(new Target[scope.size()]))
        );
    }

    @SuppressWarnings("unchecked") // We check type safety by hand.
    private <S> ConstraintCollection<S> testAndCastConstraintCollection(Class<S> type) {
        if (this.constraintCollection.getConstraintClass() != type) {
            throw new IllegalArgumentException(String.format("Conflicting constraint types: %s and %s",
                    this.constraintCollection.getConstraintClass(), type
            ));
        }
        return (ConstraintCollection<S>) this.constraintCollection;
    }

    @Override
    public void receiveResult(FunctionalDependency fd) throws CouldNotReceiveResultException {
        int[] lhs = new int[fd.getDeterminant().getColumnIdentifiers().size()];
        int i = 0;
        for (ColumnIdentifier columnIdentifier : fd.getDeterminant().getColumnIdentifiers()) {
            lhs[i++] = this.identifierResolver.resolveColumn(columnIdentifier).getId();
        }
        Arrays.sort(lhs);
        int rhs = this.identifierResolver.resolveColumn(fd.getDependant()).getId();

        this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.FunctionalDependency.class).add(
                new de.hpi.isg.mdms.domain.constraints.FunctionalDependency(lhs, rhs)
        );
    }

    @Override
    public void receiveResult(InclusionDependency ind) throws CouldNotReceiveResultException {
        int[] deps = new int[ind.getDependant().getColumnIdentifiers().size()];
        for (int i = 0; i < deps.length; i++) {
            deps[i] = this.identifierResolver.resolveColumn(ind.getDependant().getColumnIdentifiers().get(i)).getId();
        }
        int[] refs = new int[ind.getReferenced().getColumnIdentifiers().size()];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = this.identifierResolver.resolveColumn(ind.getReferenced().getColumnIdentifiers().get(i)).getId();
        }
        ReferenceUtils.coSort(deps, refs);

        this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.InclusionDependency.class).add(
                new de.hpi.isg.mdms.domain.constraints.InclusionDependency(deps, refs)
        );
    }

    @Override
    public void receiveResult(UniqueColumnCombination ucc) throws CouldNotReceiveResultException {
        int[] columns = new int[ucc.getColumnCombination().getColumnIdentifiers().size()];
        int i = 0;
        for (ColumnIdentifier columnIdentifier : ucc.getColumnCombination().getColumnIdentifiers()) {
            columns[i++] = this.identifierResolver.resolveColumn(columnIdentifier).getId();
        }
        Arrays.sort(columns);

        this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.class).add(
                new de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination(columns)
        );
    }

    @Override
    public Boolean acceptedResult(FunctionalDependency result) {
        return true;
    }

    @Override
    public Boolean acceptedResult(InclusionDependency result) {
        return true;
    }

    @Override
    public Boolean acceptedResult(UniqueColumnCombination result) {
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            this.constraintCollection.getMetadataStore().flush();
        } catch (Exception e) {
            throw new IOException("Could not flush the metadata store.", e);
        }
    }

}
