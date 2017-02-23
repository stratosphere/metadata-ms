package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.domain.experiment.RDBMSAlgorithm;
import de.hpi.isg.mdms.domain.experiment.RDBMSExperiment;
import de.hpi.isg.mdms.domain.targets.RDBMSColumn;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.domain.targets.RDBMSTable;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This interface describes common functionalities that a RDBMS-specifc interface for a {@link RDBMSMetadataStore} must
 * provide.
 *
 * @author fabian
 */
public interface SQLInterface {

    /**
     * Initializes an empty {@link de.hpi.isg.mdms.model.MetadataStore}. All Tables are dropped first if they exist. Creates all base tables
     * of a {@link RDBMSMetadataStore}.
     */
    void initializeMetadataStore() throws SQLException;

    /**
     * Writes a {@link de.hpi.isg.mdms.model.constraints.Constraint} to the constraint table.
     *
     * @param constraint should be written
     */
    <T> void writeConstraint(T constraint, ConstraintCollection<T> constraintCollection) throws SQLException;

    /**
     * Writes the given schema into the database.
     *
     * @param schema is the schema to be written
     */
    void addSchema(RDBMSSchema schema) throws SQLException;

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
    Collection<ConstraintCollection<?>> getAllConstraintCollections() throws SQLException;

    /**
     * Adds a {@link ConstraintCollection} to the database.
     *
     * @param constraintCollection is the collection to be added
     */
    void addConstraintCollection(ConstraintCollection<?> constraintCollection) throws SQLException;

    /**
     * Loads all schemas from the database.
     *
     * @return the loaded schemas
     */
    Collection<Schema> getAllSchemas() throws SQLException;

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
    Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema) throws SQLException;

    /**
     * Adds a table to the given schema.
     *
     * @param newTable is the table to add to the schema
     * @param schema   is the schema to that the table should be added
     */
    void addTableToSchema(RDBMSTable newTable, Schema schema) throws SQLException;

    /**
     * Loads all columns for a table.
     *
     * @param rdbmsTable is the table whose columns should be loaded
     * @return the loaded columns
     */
    Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable) throws SQLException;

    /**
     * Adds a column to a table.
     *
     * @param newColumn is the column that should be added
     * @param table     is the table to which the column should be added
     */
    void addColumnToTable(RDBMSColumn newColumn, Table table) throws SQLException;

    /**
     * This function ensures that all base tables needed by the {@link de.hpi.isg.mdms.model.MetadataStore} are existing.
     *
     * @return whether all required tables exist
     */
    boolean checkAllTablesExistence() throws SQLException;

    /**
     * Returns a {@link java.util.Collection} of all {@link de.hpi.isg.mdms.model.constraints.Constraint}s in a
     * {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}.
     *
     * @param rdbmsConstraintCollection is the collection whose content is requested
     * @return the constraints within the constraint collection
     */
    <T> Collection<T> getAllConstraintsForConstraintCollection(
            RDBMSConstraintCollection<T> rdbmsConstraintCollection) throws Exception;

    /**
     * Loads a column with the given ID.
     *
     * @param columnId is the ID of the column to load
     * @return the loaded column
     */
    Column getColumnById(int columnId) throws SQLException;

    /**
     * Load a table with the given ID.
     *
     * @param tableId is the ID of the table to load
     * @return the loaded table
     */
    Table getTableById(int tableId) throws SQLException;

    /**
     * Get all the {@link Table}s with the given {@code name} within the given {@link Schema}.
     *
     * @param name   the {@link Table} name
     * @param schema the {@link Schema}
     * @return the matching {@link Table}s
     */
    Collection<Table> getTablesByName(String name, Schema schema) throws SQLException;

    /**
     * Load a schema with the given ID.
     *
     * @param schemaId is the ID of the schema to load
     * @return the loaded schema
     */
    Schema getSchemaById(int schemaId) throws SQLException;

    /**
     * Returns a {@link ConstraintCollection} for a given id, <code>null</code> if no such exists.
     *
     * @param id of the constraint collection
     * @return the constraint collection
     */
    ConstraintCollection<?> getConstraintCollectionById(int id) throws SQLException;

    /**
     * Saves the configuration of a {@link de.hpi.isg.mdms.model.MetadataStore}.
     */
    void saveConfiguration();

    /**
     * Loads the {@link de.hpi.isg.mdms.model.MetadataStore} configuration from the database.
     *
     * @return a mapping from configuration keys to their values
     */
    Map<String, String> loadConfiguration() throws SQLException;

    /**
     * Load codes that are used to abbreviate common repeatedly used values.
     *
     * @return the codes
     */
    Int2ObjectMap<String> loadCodes() throws SQLException;

    /**
     * Add a code.
     *
     * @param code  the code
     * @param value the value represented by the code
     */
    void addCode(int code, String value);

    /**
     * This function drops all base tables of the {@link de.hpi.isg.mdms.model.MetadataStore}.
     */
    void dropTablesIfExist() throws SQLException;

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
    boolean tableExists(String tablename) throws SQLException;

    /**
     * This function executes a given <code>create table</code> statement.
     */
    void executeCreateTableStatement(String sqlCreateTables);

    /**
     * Returns the {@link DatabaseAccess} object that is used by this {@link SQLInterface}.
     *
     * @return the {@link DatabaseAccess} object that is used by this {@link SQLInterface}
     */
    DatabaseAccess getDatabaseAccess();

    /**
     * Loads the schemas with the given name
     *
     * @param schemaName is the name of the schema to be loaded
     * @return the loaded schemas
     */
    Collection<Schema> getSchemasByName(String schemaName) throws SQLException;

    /**
     * Removes a schema from the database.
     *
     * @param schema shall be removed
     */
    void removeSchema(RDBMSSchema schema) throws SQLException;

    /**
     * Removes a column from the database.
     *
     * @param column should be removed
     */
    void removeColumn(RDBMSColumn column) throws SQLException;

    /**
     * Removes a table from the database.
     *
     * @param table is the table that should be removed
     */
    void removeTable(RDBMSTable table) throws SQLException;

    /**
     * Removes a {@link ConstraintCollection} and all included {@link Constraint}s.
     */
    void removeConstraintCollection(ConstraintCollection<?> constraintCollection) throws SQLException;

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

    /**
     * Loads all experiments of an algorithm.
     *
     * @param rdbmsAlgorithm the algorithm whose experiments should be loaded
     * @return a collection of experiments
     */
    Collection<Experiment> getAllExperimentsForAlgorithm(RDBMSAlgorithm rdbmsAlgorithm) throws SQLException;


    /**
     * Adds an algorithm to the database.
     *
     * @param algorithm the algorithm that shall be added
     */
    void addAlgorithm(RDBMSAlgorithm algorithm) throws SQLException;


    /**
     * Adds an experiment to the database.
     *
     * @param experiment the experiment that shall be added
     */
    void writeExperiment(RDBMSExperiment experiment) throws SQLException;

    /**
     * Adds a key-value-pair of parameters to an experiment.
     *
     * @param experiment the experiment for which the parameter shall be added.
     * @param key
     * @param value
     */
    void addParameterToExperiment(RDBMSExperiment experiment,
                                  String key, String value) throws SQLException;

    /**
     * Adds the execution time to an experiment
     *
     * @param experiment    the experiment for which the execution time shall be added
     * @param executionTime in ms
     */
    void setExecutionTimeToExperiment(RDBMSExperiment experiment,
                                      long executionTime) throws SQLException;

    /**
     * Loads all constraint collections of an experiment.
     *
     * @param experiment the experiment whose constraint collections shall be loaded
     * @return a collection of constraint collections
     */
    Set<ConstraintCollection<?>> getAllConstraintCollectionsForExperiment(RDBMSExperiment experiment) throws SQLException;

    /**
     * Return an algorithm by its id
     *
     * @param algorithmId
     * @return algorithm
     */
    Algorithm getAlgorithmByID(int algorithmId) throws SQLException;

    /**
     * Return an experiment by its id
     *
     * @param experimentId
     * @return experiment
     */
    Experiment getExperimentById(int experimentId) throws SQLException;

    /**
     * Removes an algorithm and the connected experiments
     *
     * @param algorithm
     */
    void removeAlgorithm(Algorithm algorithm) throws SQLException;

    /**
     * Removes an experiment.
     *
     * @param experiment
     */
    void removeExperiment(Experiment experiment) throws SQLException;


    /**
     * Returns all algorithms
     *
     * @return collection of algorithms
     */
    Collection<Algorithm> getAllAlgorithms() throws SQLException;

    /**
     * Returns all experiments
     *
     * @return collection of experiments
     */
    Collection<Experiment> getAllExperiments() throws SQLException;

    /**
     * Returns an algorithm by name
     *
     * @param name of the algorithm
     * @return algorithm
     */
    Algorithm getAlgorithmByName(String name) throws SQLException;

    /**
     * Adds an annotation to an experiment
     *
     * @param rdbmsExperiment
     * @param tag             specifying a category of the annotation
     * @param text            message
     */
    void addAnnotation(RDBMSExperiment rdbmsExperiment, String tag, String text) throws SQLException;

    String getDatabaseURL();
}
