package de.hpi.isg.metadata_store.domain.factories;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import de.hpi.isg.metadata_store.db.DatabaseAccess;
import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.constraints.impl.ConstraintSQLSerializer;
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

    /**
     * Initializes an empty {@link MetadataStore}. All Tables are dropped first if they exist. Creates all base tables
     * of a {@link RDBMSMetadataStore} and all tables used by it's known {@link ConstraintSQLSerializer}s.
     */
    public void initializeMetadataStore();

    /**
     * Writes a {@link Constraint} to the constraint table and uses the corresponding {@link ConstraintSQLSerializer} to
     * serialize constraint specific information.
     * 
     * @param constraint
     */
    public void writeConstraint(Constraint constraint);

    /**
     * Adds a {@link RDBMSSchema} to the {@link MetadataStore}.
     * 
     * @param schema
     */
    public void addSchema(RDBMSSchema schema);

    /**
     * Returns a {@link Collection} of all known {@link Target} objects inside of the target table.
     * 
     * @return
     */
    public Collection<? extends Target> getAllTargets();

    /**
     * Checks whether a target id is in use or not.
     * 
     * @param id
     * @return
     * @throws SQLException
     */
    public boolean isTargetIdInUse(int id) throws SQLException;

    /**
     * Returns all {@link ConstraintCollection}s stored in the {@link MetadataStore}.
     * 
     * @return
     */
    public Collection<ConstraintCollection> getAllConstraintCollections();

    /**
     * Adds a {@link ConstraintCollection} to the {@link MetadataStore}.
     * 
     * @param constraintCollection
     */
    public void addConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * Returns a {@link Collection} of all {@link Schema}s.
     * 
     * @return
     */
    Collection<Schema> getAllSchemas();

    /**
     * Setter for the {@link RDBMSMetadataStore} this {@link SQLInterface} takes care of.
     * 
     * @param rdbmsMetadataStore
     */
    public void setMetadataStore(RDBMSMetadataStore rdbmsMetadataStore);

    /**
     * Getter for the {@link RDBMSMetadataStore} this {@link SQLInterface} takes care of.
     * 
     * @return
     */
    public RDBMSMetadataStore getMetadataStore();

    /**
     * Returns a {@link Collection} of all {@link Table}s for a specific {@link RDBMSSchema}.
     * 
     * @param rdbmsSchema
     * @return
     */
    public Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema);

    /**
     * Adds a {@link RDBMSTable} to a {@link Schema}.
     * 
     * @param newTable
     * @param schema
     */
    public void addTableToSchema(RDBMSTable newTable, Schema schema);

    /**
     * Returns a {@link Collection} of {@link Column}s for a given {@link RDBMSTable}.
     * 
     * @param rdbmsTable
     * @return
     */
    public Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable);

    /**
     * Adds a new {@link RDBMSColumn} to a {@link Table}.
     * 
     * @param newColumn
     * @param table
     */
    public void addColumnToTable(RDBMSColumn newColumn, Table table);

    /**
     * This function ensures that all base tables needed by the {@link MetadataStore} are existing.
     * 
     * @return
     */
    boolean allTablesExist();

    /**
     * Adds a {@link Target} object to the scope of a {@link ConstraintCollection}.
     * 
     * @param target
     * @param constraintCollection
     */
    public void addScope(Target target, ConstraintCollection constraintCollection);

    /**
     * Returns a {@link Collection} of all {@link RDBMSConstraintCollection}s.
     * 
     * @param rdbmsConstraintCollection
     * @return
     */
    public Collection<Constraint> getAllConstraintsForConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection);

    /**
     * Returns a {@link Collection} of {@link Target}s that are in the scope of a {@link ConstraintCollection}.
     * 
     * @param rdbmsConstraintCollection
     * @return
     */
    public Collection<Target> getScopeOfConstraintCollection(RDBMSConstraintCollection rdbmsConstraintCollection);

    /**
     * Returns a {@link Column} for a given id, <code>null</code> if no such exists.
     * 
     * @param columnId
     * @return
     */
    public Column getColumnById(int columnId);

    /**
     * Returns a {@link Table} for the given id, <code>null</code> if no such exists.
     * 
     * @param tableId
     * @return
     */
    public Table getTableById(int tableId);

    /**
     * Returns a {@link Schema} for a given id, <code>null</code> if no such exists.
     * 
     * @param schemaId
     * @return
     */
    public Schema getSchemaById(int schemaId);

    /**
     * Returns a {@link ConstraintCollection} for a given id, <code>null</code> if no such exists.
     * 
     * @param id
     * @return
     */
    public ConstraintCollection getConstraintCollectionById(int id);

    /**
     * Saves the configuration of a {@link MetadataStore}.
     */
    public void saveConfiguration();

    /**
     * Loads the {@link MetadataStore} configuration from the database.
     * 
     * @return
     */
    Map<String, String> loadConfiguration();

    /**
     * Returns a {@link Location} with the given id.
     * 
     * @param id
     * @return
     */
    Location getLocationFor(int id);

    /**
     * This function drops all base tables of the {@link MetadataStore}. Also all {@link ConstraintSQLSerializer} are
     * called to remove their tables.
     */
    void dropTablesIfExist();

    /**
     * Writes all pending changes back to the database.
     * 
     * @throws SQLException
     */
    void flush() throws SQLException;

    /**
     * Ensures that a particular table exists in the database.
     * 
     * @param tablename
     * @return
     */
    public boolean tableExists(String tablename);

    /**
     * This helper method must be used for creating tables, instead
     * 
     * @param sqlCreateTables
     */

    /**
     * This function executes a given <code>create table</code> statement.
     */
    void executeCreateTableStatement(String sqlCreateTables);

    /**
     * This function is used to register {@link ConstraintSQLSerializer} and therefore the ability to store and retrieve
     * the corresponding {@link Constraint} type.
     * 
     * @param clazz
     * @param serializer
     */
    void registerConstraintSQLSerializer(Class<? extends Constraint> clazz,
            ConstraintSQLSerializer<? extends Constraint> serializer);

    /**
     * Returns the {@link DatabaseAccess} object that is used by this {@link SQLInterface}.
     * 
     * @return
     */
    public DatabaseAccess getDatabaseAccess();

    public Schema getSchemaByName(String schemaName) throws NameAmbigousException;

    public Collection<Schema> getSchemasByName(String schemaName);

    public Collection<Column> getColumnsByName(String name);

    public Column getColumnByName(String name, Table rdbmsTable) throws NameAmbigousException;

    public Table getTableByName(String name) throws NameAmbigousException;

    public Collection<Table> getTablesByName(String name);

    public void removeSchema(RDBMSSchema schema);

    public void removeColumn(RDBMSColumn column);

    public void removeTable(RDBMSTable table);

    /**
     * Removes a {@link ConstraintCollection} and all included {@link Constraint}s.
     * 
     * @param constraintCollection
     */
    public void removeConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * @return all stored {@link Location} types
     * @throws SQLException
     */
    public Collection<String> getLocationClassNames() throws SQLException;

    /**
     * Stores a given {@link Location} type.
     * 
     * @param locationType
     *        is the type of a {@link Location} to be stored
     * @throws SQLException
     */
    public void storeLocationType(Class<? extends Location> locationType) throws SQLException;
}
