package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.domain.targets.RDBMSColumn;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.domain.targets.RDBMSTable;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/**
 * This interface describes common functionalities that a RDBMS-specifc interface for a {@link RDBMSMetadataStore} must
 * provide.
 *
 * @author fabian
 */
public interface SQLInterface {

    /**
     * Initializes an empty {@link de.hpi.isg.mdms.model.MetadataStore}. All Tables are dropped first if they exist. Creates all base tables
     * of a {@link RDBMSMetadataStore} and all tables used by it's known {@link ConstraintSQLSerializer}s.
     */
    void initializeMetadataStore();

    /**
     * Writes a {@link de.hpi.isg.mdms.model.constraints.Constraint} to the constraint table and uses the corresponding {@link ConstraintSQLSerializer} to
     * serialize constraint specific information.
     *
     * @param constraint should be written
     */
    void writeConstraint(Constraint constraint, ConstraintCollection constraintCollection);

    /**
     * Writes the given schema into the database.
     *
     * @param schema is the schema to be written
     */
    void addSchema(RDBMSSchema schema);

    /**
     * Returns all {@link de.hpi.isg.mdms.model.targets.Target}s from the underlying database.
     *
     * @return the targets
     */
    Collection<? extends Target> getAllTargets();

    /**
     * Checks whether there exists a schema element with the given ID.
     *
     * @param id is the ID of the questionnable schema element
     * @return whether the schema element exists
     * @throws SQLException
     */
    boolean isTargetIdInUse(int id) throws SQLException;

    /**
     * Returns all {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}s stored in the {@link de.hpi.isg.mdms.model.MetadataStore}.
     *
     * @return the constraint collections
     */
    Collection<ConstraintCollection> getAllConstraintCollections();

    /**
     * Adds a {@link ConstraintCollection} to the database.
     *
     * @param constraintCollection is the collection to be added
     */
    void addConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * Loads all schemas from the database.
     *
     * @return the loaded schemas
     */
    Collection<Schema> getAllSchemas();

    /**
     * Getter for the {@link RDBMSMetadataStore} this {@link SQLInterface} takes care of.
     *
     * @return the metadata store
     */
    RDBMSMetadataStore getMetadataStore();

    /**
     * Setter for the {@link RDBMSMetadataStore} this {@link SQLInterface} takes care of.
     */
    void setMetadataStore(RDBMSMetadataStore rdbmsMetadataStore);

    /**
     * Loads all tables for a schema.
     *
     * @param rdbmsSchema is the schema whose tables shall be loaded
     * @return the tables of the schema
     */
    Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema);

    /**
     * Adds a table to the given schema.
     *
     * @param newTable is the table to add to the schema
     * @param schema   is the schema to that the table should be added
     */
    void addTableToSchema(RDBMSTable newTable, Schema schema);

    /**
     * Loads all columns for a table.
     *
     * @param rdbmsTable is the table whose columns should be loaded
     * @return the loaded columns
     */
    Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable);

    /**
     * Adds a column to a table.
     *
     * @param newColumn is the column that should be added
     * @param table     is the table to which the column should be added
     */
    void addColumnToTable(RDBMSColumn newColumn, Table table);

    /**
     * This function ensures that all base tables needed by the {@link de.hpi.isg.mdms.model.MetadataStore} are existing.
     *
     * @return whether all required tables exist
     */
    boolean allTablesExist();

    /**
     * Adds a {@link Target} object to the scope of a {@link ConstraintCollection}.
     *
     * @param target to be added
     * @param constraintCollection to whose scope the target should be added
     */
    void addScope(Target target, ConstraintCollection constraintCollection);

    /**
     * Returns a {@link java.util.Collection} of all {@link de.hpi.isg.mdms.model.constraints.Constraint}s in a
     * {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}.
     *
     * @param rdbmsConstraintCollection is the collection whose content is requested
     * @return the constraints within the constraint collection
     */
    Collection<Constraint> getAllConstraintsForConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection);

    /**
     * Returns a {@link java.util.Collection} of {@link Target}s that are in the scope of a {@link ConstraintCollection}.
     *
     * @param rdbmsConstraintCollection holds the scope
     * @return the schema elements within the scope
     */
    Collection<Target> getScopeOfConstraintCollection(RDBMSConstraintCollection rdbmsConstraintCollection);

    /**
     * Loads a column with the given ID.
     *
     * @param columnId is the ID of the column to load
     * @return the loaded column
     */
    Column getColumnById(int columnId);

    /**
     * Load a table with the given ID.
     *
     * @param tableId is the ID of the table to load
     * @return the loaded table
     */
    Table getTableById(int tableId);

    /**
     * Load a schema with the given ID.
     *
     * @param schemaId is the ID of the schema to load
     * @return the loaded schema
     */
    Schema getSchemaById(int schemaId);

    /**
     * Returns a {@link ConstraintCollection} for a given id, <code>null</code> if no such exists.
     *
     * @param id of the constraint collection
     * @return the constraint collection
     */
    ConstraintCollection getConstraintCollectionById(int id);

    /**
     * Saves the configuration of a {@link de.hpi.isg.mdms.model.MetadataStore}.
     */
    void saveConfiguration();

    /**
     * Loads the {@link de.hpi.isg.mdms.model.MetadataStore} configuration from the database.
     *
     * @return a mapping from configuration keys to their values
     */
    Map<String, String> loadConfiguration();

    /**
     * Loads the {@link de.hpi.isg.mdms.model.location.Location} for the schema element with the given ID.
     *
     * @param id is the ID of the schema element for that the location should be loaded
     * @return the loaded location
     */
    Location getLocationFor(int id);

    /**
     * This function drops all base tables of the {@link de.hpi.isg.mdms.model.MetadataStore}. Also all {@link ConstraintSQLSerializer} are
     * called to remove their tables.
     */
    void dropTablesIfExist();

    /**
     * Writes all pending changes back to the database.
     *
     * @throws java.sql.SQLException
     */
    void flush() throws SQLException;

    /**
     * Ensures that a particular table exists in the database.
     *
     * @param tablename the name of the table whose existance shall be verified
     * @return whether the table exists
     */
    boolean tableExists(String tablename);

    /**
     * This function executes a given <code>create table</code> statement.
     */
    void executeCreateTableStatement(String sqlCreateTables);

    /**
     * This function is used to register {@link ConstraintSQLSerializer} and therefore the ability to store and retrieve
     * the corresponding {@link Constraint} type.
     *
     * @param clazz is the type of the constraint
     * @param serializer that can serialize the constraints of the given class
     */
    void registerConstraintSQLSerializer(Class<? extends Constraint> clazz,
                                         ConstraintSQLSerializer<? extends Constraint> serializer);

    /**
     * Returns the {@link DatabaseAccess} object that is used by this {@link SQLInterface}.
     *
     * @return the {@link DatabaseAccess} object that is used by this {@link SQLInterface}
     */
    DatabaseAccess getDatabaseAccess();

    /**
     * Tries to load a schema with the given name.
     *
     * @param schemaName is the name of the schema to load
     * @return the loaded schema or {@code null} if there is no such schema
     * @throws NameAmbigousException if there are multiple schemata with the given name
     */
    Schema getSchemaByName(String schemaName) throws NameAmbigousException;

    /**
     * Loads the schemas with the given name
     *
     * @param schemaName is the name of the schema to be loaded
     * @return the loaded schemas
     */
    Collection<Schema> getSchemasByName(String schemaName);

    /**
     * Loads the columns with the given name.
     *
     * @param columnName is the name of the columns to load
     * @return the loaded columns
     */
    Collection<Column> getColumnsByName(String columnName);

    /**
     * Loads the column with the given name.
     *
     * @param columnName is the name of the column to load
     * @param table      is the table that should contain the column
     * @return the column or {@code null} if no such column exists
     * @throws NameAmbigousException if there is more than one such column within the given table
     */
    Column getColumnByName(String columnName, Table table) throws NameAmbigousException;

    /**
     * Loads the table with the given name.
     *
     * @param tableName is the name of the table to load
     * @return the loaded table or {@code null} if no such table exists
     * @throws NameAmbigousException if there is more than table with the given name
     */
    Table getTableByName(String tableName) throws NameAmbigousException;

    /**
     * Loads the tables with the given name.
     *
     * @param tableName is the name of the table to be loaded
     * @return the loaded tables
     */
    Collection<Table> getTablesByName(String tableName);

    /**
     * Removes a schema from the database.
     *
     * @param schema shall be removed
     */
    void removeSchema(RDBMSSchema schema);

    /**
     * Removes a column from the database.
     *
     * @param column should be removed
     */
    void removeColumn(RDBMSColumn column);

    /**
     * Removes a table from the database.
     *
     * @param table is the table that should be removed
     */
    void removeTable(RDBMSTable table);

    /**
     * Removes a {@link ConstraintCollection} and all included {@link Constraint}s.
     */
    void removeConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * @return all stored {@link Location} types
     * @throws java.sql.SQLException
     */
    Collection<String> getLocationClassNames() throws SQLException;

    /**
     * Stores a given {@link Location} type.
     *
     * @param locationType is the type of a {@link Location} to be stored
     * @throws java.sql.SQLException
     */
    void storeLocationType(Class<? extends Location> locationType) throws SQLException;

    /**
     * Set whether the underlying DB should use journaling. Note that this experimental feature should not affect
     * the correctness of operations and might not be supported by DBs.
     *
     * @param isUseJournal tells whether to use journaling
     */
    void setUseJournal(boolean isUseJournal);

    /**
     * Closes the the connection to the underlying database.
     */
    void closeMetaDataStore();
    
    /**
     * An enumeration of DBs supported by default.
     */
    enum RDBMS {
        SQLITE
    }

	public String getDatabaseURL();
}
