package de.hpi.isg.mdms.tools.metanome;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.*;

import java.io.IOException;
import java.util.Collection;

/**
 * Writes all received results to a specified {@link MetadataStore}.
 * Receives the Metanome results and extracts the relevant information.
 *
 * @author Susanne Buelow
 * @author Sebastian Kruse
 */
public class DependencyResultReceiver<T extends Constraint> implements AutoCloseable,
        UniqueColumnCombinationResultReceiver, FunctionalDependencyResultReceiver, InclusionDependencyResultReceiver{

    private final ConstraintCollection<T> constraintCollection;

    private final IdentifierResolver identifierResolver;

    public DependencyResultReceiver(MetadataStore metadatastore,
                                    Schema schema,
                                    Collection<Target> scope,
                                    Class<T> constraintClass,
                                    String resultDescription) {
        this.identifierResolver = new IdentifierResolver(metadatastore, schema);
        this.constraintCollection = metadatastore.createConstraintCollection(
                resultDescription, constraintClass, scope.toArray(scope.toArray(new Target[scope.size()]))
        );
    }

    @SuppressWarnings("unchecked") // We check type safety by hand.
    private <S extends Constraint> ConstraintCollection<S> testAndCastConstraintCollection(Class<S> type) {
        if (this.constraintCollection.getConstraintClass() != type) {
            throw new IllegalArgumentException(String.format("Conflicting constraint types: %s and %s",
                    this.constraintCollection.getConstraintClass(), type
            ));
        }
        return (ConstraintCollection<S>) this.constraintCollection;
    }

    @Override
    public void receiveResult(FunctionalDependency fd) throws CouldNotReceiveResultException {
        Column[] lhs = new Column[fd.getDeterminant().getColumnIdentifiers().size()];
        int i = 0;
        for (ColumnIdentifier columnIdentifier : fd.getDeterminant().getColumnIdentifiers()) {
            lhs[i++] = this.identifierResolver.resolveColumn(columnIdentifier);
        }
        Column rhs = this.identifierResolver.resolveColumn(fd.getDependant());

        de.hpi.isg.mdms.domain.constraints.FunctionalDependency.buildAndAddToCollection(
                new de.hpi.isg.mdms.domain.constraints.FunctionalDependency.Reference(rhs, lhs),
                this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.FunctionalDependency.class)
        );
    }

    @Override
    public void receiveResult(InclusionDependency ind) throws CouldNotReceiveResultException {
        Column[] deps = new Column[ind.getDependant().getColumnIdentifiers().size()];
        for (int i = 0; i < deps.length; i++) {
            deps[i] = this.identifierResolver.resolveColumn(ind.getDependant().getColumnIdentifiers().get(i));
        }
        Column[] refs = new Column[ind.getReferenced().getColumnIdentifiers().size()];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = this.identifierResolver.resolveColumn(ind.getReferenced().getColumnIdentifiers().get(i));
        }

        de.hpi.isg.mdms.domain.constraints.InclusionDependency.buildAndAddToCollection(
                new de.hpi.isg.mdms.domain.constraints.InclusionDependency.Reference(deps, refs),
                this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.InclusionDependency.class)
        );
    }

    @Override
    public void receiveResult(UniqueColumnCombination ucc) throws CouldNotReceiveResultException {
        Column[] columns = new Column[ucc.getColumnCombination().getColumnIdentifiers().size()];
        int i = 0;
        for (ColumnIdentifier columnIdentifier : ucc.getColumnCombination().getColumnIdentifiers()) {
            columns[i++] = this.identifierResolver.resolveColumn(columnIdentifier);
        }

        de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.buildAndAddToCollection(
                new de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.Reference(columns),
                this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.class)
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
