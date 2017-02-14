package de.hpi.isg.mdms.tools.metanome;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.*;
import de.metanome.backend.result_receiver.ResultReceiver;
import org.apache.commons.lang.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * Writes all received results to a specified {@link MetadataStore}.
 * Receives the Metanome results and extracts the relevant information.
 *
 * @author Susanne Buelow
 * @author Sebastian Kruse
 */
public class ResultMetadataStoreWriter<T extends Constraint> extends ResultReceiver {

    private final ConstraintCollection<T> constraintCollection;

    /**
     * Metanome does not have the notion of schemata, so we require to have a given {@link Schema} which the
     * imported metadata describe.
     */
    private final Schema schema;

    public ResultMetadataStoreWriter(String algorithmExecutionIdentifier,
                                     MetadataStore metadatastore,
                                     Schema schema,
                                     Collection<Target> scope,
                                     Class<T> constraintClass,
                                     String resultDescription) throws FileNotFoundException {
        super(algorithmExecutionIdentifier);
        this.schema = schema;
        this.constraintCollection = metadatastore.createConstraintCollection(
                resultDescription, constraintClass, scope.toArray(new Target[scope.size()])
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

    /**
     * TODO: Method not implemented yet!
     */
    @Override
    public void receiveResult(BasicStatistic statistic) {
        throw new NotImplementedException();
    }

    @Override
    public void receiveResult(FunctionalDependency fd) {
        Column[] lhs = new Column[fd.getDeterminant().getColumnIdentifiers().size()];
        int i = 0;
        for (ColumnIdentifier columnIdentifier : fd.getDeterminant().getColumnIdentifiers()) {
            lhs[i++] = this.resolveColumn(columnIdentifier);
        }
        Column rhs = this.resolveColumn(fd.getDependant());

        de.hpi.isg.mdms.domain.constraints.FunctionalDependency.buildAndAddToCollection(
                new de.hpi.isg.mdms.domain.constraints.FunctionalDependency.Reference(rhs, lhs),
                this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.FunctionalDependency.class)
        );
    }

    @Override
    public void receiveResult(InclusionDependency ind) {
        Column[] deps = new Column[ind.getDependant().getColumnIdentifiers().size()];
        for (int i = 0; i < deps.length; i++) {
            deps[i] = this.resolveColumn(ind.getDependant().getColumnIdentifiers().get(i));
        }
        Column[] refs = new Column[ind.getReferenced().getColumnIdentifiers().size()];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = this.resolveColumn(ind.getReferenced().getColumnIdentifiers().get(i));
        }

        de.hpi.isg.mdms.domain.constraints.InclusionDependency.buildAndAddToCollection(
                new de.hpi.isg.mdms.domain.constraints.InclusionDependency.Reference(deps, refs),
                this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.InclusionDependency.class)
        );
    }

    @Override
    public void receiveResult(UniqueColumnCombination ucc) {
        Column[] columns = new Column[ucc.getColumnCombination().getColumnIdentifiers().size()];
        int i = 0;
        for (ColumnIdentifier columnIdentifier : ucc.getColumnCombination().getColumnIdentifiers()) {
            columns[i++] = this.resolveColumn(columnIdentifier);
        }

        de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.buildAndAddToCollection(
                new de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.Reference(columns),
                this.testAndCastConstraintCollection(de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination.class)
        );
    }

    /**
     * TODO: Method not implemented yet!
     */
    public void receiveResult(ConditionalUniqueColumnCombination conditionalUniqueColumnCombination) {
        throw new NotImplementedException();
    }

    /**
     * TODO: Method not implemented yet!
     */
    @Override
    public void receiveResult(OrderDependency orderDependency) throws CouldNotReceiveResultException {
        throw new NotImplementedException();
    }

    /**
     * Resolve a {@link ColumnIdentifier} to a {@link Column}.
     *
     * @throws IllegalArgumentException if the {@link Column} could not be found
     */
    private Column resolveColumn(ColumnIdentifier columnIdentifier) throws IllegalArgumentException {
        Table table = this.resolveTable(columnIdentifier.getTableIdentifier());
        return this.resolveColumn(table, columnIdentifier.getColumnIdentifier());
    }

    private Column resolveColumn(Table table, String columnIdentifier) {
        Column column = table.getColumnByName(columnIdentifier);
        if (column != null) return column;

        // Detect Metanome's fallback column names.
        if (columnIdentifier.startsWith("column")) {
            try {
                int columnPosition = Integer.parseInt(columnIdentifier.substring(7));
                IdUtils idUtils = this.constraintCollection.getMetadataStore().getIdUtils();
                int localSchemaId = idUtils.getLocalSchemaId(table.getId());
                int localTableId = idUtils.getLocalTableId(table.getId());
                int columnId = idUtils.createGlobalId(localSchemaId, localTableId, columnPosition - 1);
                column = table.getColumnById(columnId);
                if (column != null) return column;
            } catch (NumberFormatException e) {
                // Pass.
            }
        }

        throw new IllegalArgumentException(String.format("Cannot find a column named \"%s\" in %s.", columnIdentifier, table));
    }

    /**
     * Resolves a {@link Table} by its name.
     *
     * @param tableIdentifier the name of the {@link Table}
     * @return the {@link Table}
     * @throws IllegalArgumentException if the {@link Table} could not be found
     */
    private Table resolveTable(String tableIdentifier) throws IllegalArgumentException {
        // Try to resolve the table name immediately.
        Table table = this.schema.getTableByName(tableIdentifier);
        if (table != null) return table;

        // Try to strip of any file extension comprised in the table name.
        int extensionIndex = tableIdentifier.lastIndexOf('.');
        if (extensionIndex != -1) {
            String trimmedTableIdentifier = tableIdentifier.substring(0, extensionIndex);
            table = this.schema.getTableByName(trimmedTableIdentifier);
            if (table != null) return table;
        }

        throw new IllegalArgumentException(String.format("Cannot find a table named \"%s\".", tableIdentifier));
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
