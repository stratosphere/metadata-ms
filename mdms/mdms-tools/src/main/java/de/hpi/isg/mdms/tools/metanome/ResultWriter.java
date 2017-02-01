package de.hpi.isg.mdms.tools.metanome;

import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.apache.flink.shaded.com.google.common.collect.Iterables;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Writes dependencies to the metadatastore.
 * It receives necessary information about dependencies and transforms them to a format required for the metadatastore.
 *
 * @author Susanne Buelow
 * @author Sebastian Kruse
 */

public class ResultWriter {

    private final MetadataStore metadataStore;
    private Schema schema;
    private final String tableName;
    private final HashMap<String, Table> tables = new HashMap<String, Table>();
    private final HashMap<String, HashMap<String, Column>> columns = new HashMap<String, HashMap<String, Column>>();
    private final String description;
    private ConstraintCollection<?> constraintCollection;

    public ResultWriter(MetadataStoreParameters metadatastoreParameters, String schemaname, String tablename, String description) {
        this(MetadataStoreUtil.loadMetadataStore(metadatastoreParameters), schemaname, tablename, description);
    }

    public ResultWriter(MetadataStore metadatastore, String schemaname, String tablename, String description) {
        this.metadataStore = metadatastore;
        this.tableName = tablename;
        for (final Schema schema : this.metadataStore.getSchemas()) {
            if (schemaname != null) {
                if (schemaname.equals(schema.getName()) || schemaname.equals(schema.getId())) {
                    this.schema = schema;
                }
            }
        }
        for (Table table : this.schema.getTables()) {
            this.tables.put(table.getName(), table);
            HashMap<String, Column> columnsForTable = new HashMap<String, Column>();
            for (Column column : table.getColumns()) {
                columnsForTable.put(column.getName(), column);
            }
            this.columns.put(table.getName(), columnsForTable);
        }

        this.description = description;
    }

    @SuppressWarnings("unchecked") // We check type safety by hand.
    private <T extends Constraint> ConstraintCollection<T> getConstraintCollection(Class<T> type) {
        if (this.constraintCollection == null) {
            String constraintsDescription = String.format(this.description,
                    this.schema.getName(), DateFormat.getInstance().format(new Date()));
            ConstraintCollection<T> constraintCollection = this.metadataStore.createConstraintCollection(constraintsDescription, type, this.schema);
            this.constraintCollection = constraintCollection;
            return constraintCollection;
        } else {
            // Ensure that the constraint collection has the right type.
            if (type != this.constraintCollection.getConstraintClass()) {
                throw new IllegalArgumentException(String.format("Conflicting constraint types: %s and %s",
                        this.constraintCollection.getConstraintClass(), type
                ));
            }
            return (ConstraintCollection<T>) this.constraintCollection;
        }
    }

    /**
     * Writes a functional dependency to the metadatastore.
     *
     * @param fdRHSTableName   table name of the right-hand-side of the functional dependency
     * @param fdRHSColumnName  column name of the right-hand-side of the functional dependency
     * @param fdLHSTableNames  list of table names of the left-hand-side of the functional dependency
     * @param fdLHSColumnNames list of column names of the legt-hand-side of the functional dependency
     */
    public void writeFD(String fdRHSTableName, String fdRHSColumnName, String[] fdLHSTableNames, String[] fdLHSColumnNames) {

        int fdRHSColumnId = getColumnIdFor(fdRHSTableName, fdRHSColumnName);

        int[] fdLHSColumnIds = new int[fdLHSColumnNames.length];
        //translate names to column ids from metadatastore
        for (int i = 0; i < fdLHSColumnNames.length; i++) {
            fdLHSColumnIds[i] = getColumnIdFor(fdLHSTableNames[i], fdLHSColumnNames[i]);
        }

        synchronized (this) {
            ConstraintCollection<FunctionalDependency> constraintCollection = this.getConstraintCollection(FunctionalDependency.class);
            FunctionalDependency.buildAndAddToCollection(
                    new FunctionalDependency.Reference(fdRHSColumnId, fdLHSColumnIds),
                    constraintCollection
            );
        }
    }

    /**
     * Writes a unique column combination to the metadatastore.
     *
     * @param uniqueColumnTableNames list of table names of the unique column combination
     * @param uniqueColumnNames      list of column names of the unique column combination
     */
    public void writeUCC(String[] uniqueColumnTableNames, String[] uniqueColumnNames) {
        int[] uniqueColumnIds = new int[uniqueColumnNames.length];
        //translate names to column ids from metadatastore
        for (int i = 0; i < uniqueColumnNames.length; i++) {
            uniqueColumnIds[i] = getColumnIdFor(uniqueColumnTableNames[i], uniqueColumnNames[i]);
        }

        synchronized (this) {
            ConstraintCollection<UniqueColumnCombination> constraintCollection = this.getConstraintCollection(UniqueColumnCombination.class);
            UniqueColumnCombination.buildAndAddToCollection(new UniqueColumnCombination.Reference(uniqueColumnIds), constraintCollection);
        }
    }

    /**
     * Writes an inclusion dependency to the metadatastore.
     *
     * @param refTableNames  list of table names of the referenced columns
     * @param refColumnNames list of column names of the referenced columns
     * @param depTableNames  list of table names of the dependent columns
     * @param depColumnNames list of column names of the dependent columns
     */
    public void writeIND(String[] refTableNames, String[] refColumnNames, String[] depTableNames, String[] depColumnNames) {
        Column[] referencedColumns = new Column[refTableNames.length];
        Column[] dependentColumns = new Column[depTableNames.length];
        for (int i = 0; i < refTableNames.length; i++) {
            referencedColumns[i] = getColumnFor(refTableNames[i], refColumnNames[i]);
            dependentColumns[i] = getColumnFor(depTableNames[i], depColumnNames[i]);
        }

        synchronized (this) {
            ConstraintCollection<InclusionDependency> constraintCollection = this.getConstraintCollection(InclusionDependency.class);
            InclusionDependency.buildAndAddToCollection(
                    new InclusionDependency.Reference(dependentColumns, referencedColumns),
                    constraintCollection
            );
        }
    }

    private Column getColumnFor(String tableName, String columnName) {
        Table table;
        if (!tableName.isEmpty()) {
            table = this.tables.get(tableName);
            if (table == null && !tableName.endsWith(".csv")) {
                tableName = tableName + ".csv";
                table = this.tables.get(tableName);
            }
        } else {
            if (this.tableName != null) {
                table = this.tables.get(this.tableName);
            } else {
                table = Iterables.get(this.schema.getTables(), 0);
            }
        }
        if (table == null)
            throw new IllegalArgumentException("There is no table \"" + tableName + "\" in the schema \"" + this.schema.getName() + "\"");
        HashMap<String, Column> columnsOfTable = this.columns.get(table.getName());
        return columnsOfTable.get(columnName);
    }

    private int getColumnIdFor(String tableName, String columnName) {
        Column column = getColumnFor(tableName, columnName);
        if (column == null) {
            String msg = String.format("Could not find a column \"%s\" in table \"%s\".", columnName, tableName);
            throw new IllegalArgumentException(msg);
        }
        return column.getId();
    }

    public void close() {
        try {
            this.metadataStore.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.metadataStore.close();

    }

}
