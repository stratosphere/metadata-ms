package de.hpi.isg.metadata_store.domain.factories;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

import de.hpi.isg.metadata_store.db.DatabaseAccess;
import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.constraints.impl.ConstraintSQLSerializer;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency.Reference;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSTable;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * This interface describes common functionalities that a RDBMS-specifc interface for a {@link RDBMSMetadataStore} must
 * provide.
 * 
 * @author fabian
 *
 */
public interface SQLInterface {

    public static enum RDBMS {
        SQLITE
    };

    public void initializeMetadataStore();

    public void addConstraint(Constraint constraint);

    public void addSchema(Schema schema);

    public Collection<? extends Target> getAllTargets();

    public boolean isTargetIdInUse(int id) throws SQLException;

    // public void addTarget(Target target);

    public Collection<ConstraintCollection> getAllConstraintCollections();

    public void addConstraintCollection(ConstraintCollection constraintCollection);

    Collection<Schema> getAllSchemas();

    public void setMetadataStore(RDBMSMetadataStore rdbmsMetadataStore);

    // boolean addToIdsInUse(int id);

    public Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema);

    public void addTableToSchema(RDBMSTable newTable, Schema schema);

    public Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable);

    public void addColumnToTable(RDBMSColumn newColumn, Table table);

    boolean allTablesExist();

    public void addScope(Target target, ConstraintCollection constraintCollection);

    public Collection<Constraint> getAllConstraintsOrOfConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection);

    public Collection<Target> getScopeOfConstraintCollection(RDBMSConstraintCollection rdbmsConstraintCollection);

    public Column getColumnById(int columnId);

    public Table getTableById(int tableId);

    Schema getSchemaById(int schemaId);

    ConstraintCollection getConstraintCollectionById(int id);

    void saveConfiguration();

    Map<String, String> loadConfiguration();

    Location getLocationFor(int id);

    void dropTablesIfExist();

    void flush() throws SQLException;

    @Deprecated
    public Statement createStatement() throws SQLException;

    public boolean tableExists(String tablename);

    /**
     * This helper method must be used for creating tables, instead
     * 
     * @param sqlCreateTables
     */

    void executeCreateTableStatement(String sqlCreateTables);

    void registerConstraintSQLSerializer(Class<? extends Constraint> clazz, ConstraintSQLSerializer serializer);

    public Schema getSchemaByName(String schemaName) throws NameAmbigousException;

    public Collection<Schema> getSchemasByName(String schemaName);

    public Collection<Column> getColumnsByName(String name);

    public Column getColumnByName(String name) throws NameAmbigousException;

    public Table getTableByName(String name) throws NameAmbigousException;

    public Collection<Table> getTablesByName(String name);

    public DatabaseAccess getDatabaseAccess();
}
