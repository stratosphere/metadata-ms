package de.hpi.isg.mdms.rdbms;

import com.twitter.chill.KryoPool;
import com.twitter.chill.ScalaKryoInstantiator;
import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.domain.experiment.RDBMSAlgorithm;
import de.hpi.isg.mdms.domain.experiment.RDBMSExperiment;
import de.hpi.isg.mdms.domain.targets.RDBMSColumn;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.domain.targets.RDBMSTable;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * This class acts as an executor of SQLite specific Queries for the {@link de.hpi.isg.mdms.domain.RDBMSMetadataStore}.
 *
 * @author fabian
 */
public class SQLiteInterface implements SQLInterface {

    /**
     * See {@code persistence_sqlite.sql}
     */
    public static final String[] tableNames = {
            "Target", "ConstraintCollection", "Constraint",
            "Config", "Code",
            "Experiment", "Algorithm", "ExperimentParameter", "Annotation"
    };

    private static final Logger LOG = LoggerFactory.getLogger(SQLInterface.class);

    /**
     * Resource path of the SQL script to set up the metadata metadataStore schema.
     */
    private static final String SETUP_SCRIPT_RESOURCE_PATH = "/sqlite/persistence_sqlite.sql";

    /**
     * Encapsulates the DB connection to allow for lazy writes.
     */
    private final DatabaseAccess databaseAccess;

    RDBMSMetadataStore store;

    /**
     * Used to read and write the {@code data} fields.
     */
    private final KryoPool kryoPool = KryoPool.withByteArrayOutputStream(4, new ScalaKryoInstantiator());

    /**
     * Keeps track of existing tables within the DB.
     */
    private Set<String> existingTables;

    private SQLiteSchemaHandler schemaHandler;

    private SQLiteConstraintHandler constraintHandler;

    private SQLiteExperimentHandler experimentHandler;

    /**
     * Creates a new instance that operates on the given connection.
     *
     * @param connection to operate on
     */
    public SQLiteInterface(Connection connection) throws SQLException {
        this.databaseAccess = new DatabaseAccess(connection);
        this.schemaHandler = new SQLiteSchemaHandler(this.databaseAccess, this.kryoPool);
        this.constraintHandler = new SQLiteConstraintHandler(this, this.kryoPool);
        this.experimentHandler = new SQLiteExperimentHandler(this);

    }

    /**
     * Creates a SQLiteInterface for the SQLite DB that is embedded in the given file.
     *
     * @param file is the file that contains the SQLite DB
     * @return the SQLiteInterface
     */
    public static SQLiteInterface createForFile(File file) {
        try {
            Class.forName("org.sqlite.JDBC");
            String connString = String.format("jdbc:sqlite:%s", file.getAbsoluteFile());
            Connection connection = DriverManager.getConnection(connString);
            return new SQLiteInterface(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initializeMetadataStore() throws SQLException {
        // Drop any old tables.
        this.dropTablesIfExist();

        // Create the DB schema.
        try {
            String sqlCreateTables = loadResource(SETUP_SCRIPT_RESOURCE_PATH);
            this.executeCreateTableStatement(sqlCreateTables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            this.flush();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads the given resource as String.
     *
     * @param resourcePath is the path of the resource
     * @return a {@link String} with the contents of the resource
     * @throws java.io.IOException
     */
    private static String loadResource(String resourcePath) throws IOException {
        try (InputStream resourceStream = SQLiteInterface.class.getResourceAsStream(resourcePath)) {
            return IOUtils.toString(resourceStream, "UTF-8");
        }
    }

    /**
     * Loads the tables that exist within the DB.
     */
    private void loadTableNames() throws SQLException {
        this.existingTables = new HashSet<>();
        DatabaseMetaData meta;
        meta = this.databaseAccess.getConnection().getMetaData();
        try (ResultSet res = meta.getTables(null, null, null, new String[]{"TABLE"})) {
            while (res.next()) {
                // toLowerCase because SQLite is case-insensitive for table names
                this.existingTables.add(res.getString("TABLE_NAME").toLowerCase());
            }
        }
    }

    @Override
    public boolean checkAllTablesExistence() throws SQLException {
        for (String tableName : tableNames) {
            // toLowerCase because SQLite is case-insensitive for table names
            if (!this.tableExists(tableName.toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addSchema(RDBMSSchema schema) throws SQLException {
        this.schemaHandler.writeSchema(schema);
    }


    @Override
    public boolean isTargetIdInUse(int id) throws SQLException {
        return this.schemaHandler.isTargetIdInUse(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ConstraintCollection<? extends Constraint>> getAllConstraintCollections() throws SQLException {
        return this.constraintHandler.getAllConstraintCollections();
    }

    @Override
    public void addConstraintCollection(ConstraintCollection<? extends Constraint> constraintCollection) throws SQLException {
        this.constraintHandler.addConstraintCollection(constraintCollection);
    }

    /**
     * @see SQLiteSchemaHandler#getAllSchemas()
     */
    @Override
    public Collection<Schema> getAllSchemas() throws SQLException {
        return this.schemaHandler.getAllSchemas();
    }

    @Override
    public RDBMSMetadataStore getMetadataStore() {
        return this.store;
    }

    @Override
    public void setMetadataStore(RDBMSMetadataStore rdbmsMetadataStore) {
        this.store = rdbmsMetadataStore;
        this.schemaHandler.setMetadataStore(rdbmsMetadataStore);
        this.constraintHandler.setMetadataStore(rdbmsMetadataStore);
    }

    @Override
    public Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema) throws SQLException {
        return this.schemaHandler.getTables(rdbmsSchema);
    }

    @Override
    public void addTableToSchema(RDBMSTable newTable, Schema schema) throws SQLException {
        this.schemaHandler.writeTable(newTable);
    }

    @Override
    public Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable) throws SQLException {
        return this.schemaHandler.getColumns(rdbmsTable);
    }

    @Override
    public void addColumnToTable(RDBMSColumn newColumn, Table table) throws SQLException {
        this.schemaHandler.writeColumn(newColumn);
    }

    @Override
    public <T extends Constraint> Collection<T> getAllConstraintsForConstraintCollection(
            RDBMSConstraintCollection<T> rdbmsConstraintCollection) throws Exception {
        return this.constraintHandler.getAllConstraintsForConstraintCollection(rdbmsConstraintCollection);
    }

    @Override
    public Column getColumnById(int columnId) throws SQLException {
        return this.schemaHandler.getColumnById(columnId);
    }

    @Override
    public Table getTableById(int tableId) throws SQLException {
        return this.schemaHandler.getTableById(tableId);
    }

    @Override
    public Collection<Table> getTablesByName(String name, Schema schema) throws SQLException {
        return this.schemaHandler.getTables(name, schema);
    }

    @Override
    public Schema getSchemaById(int schemaId) throws SQLException {
        return this.schemaHandler.getSchemaById(schemaId);
    }

    @Override
    public ConstraintCollection<? extends Constraint> getConstraintCollectionById(int id) throws SQLException {
        return this.constraintHandler.getConstraintCollectionById(id);
    }

    @Override
    public <T extends Constraint> void writeConstraint(T constraint, ConstraintCollection<T> constraintCollection) throws SQLException {
        this.constraintHandler.writeConstraint(constraint, (RDBMSConstraintCollection<T>) constraintCollection);
    }

    /**
     * Saves configuration of the metadata metadataStore.
     */
    @Override
    public void saveConfiguration() {
        try {
            Map<String, String> configuration = this.store.getConfiguration();
            for (Map.Entry<String, String> configEntry : configuration.entrySet()) {
                String configKey = configEntry.getKey();
                String value = configEntry.getValue();
                this.databaseAccess.executeSQL(
                        String.format("DELETE FROM [Config] WHERE [key]=\"%s\";", configKey),
                        "Config");
                this.databaseAccess.executeSQL(
                        String.format("INSERT INTO [Config] ([key], [value]) VALUES (\"%s\", \"%s\");",
                                configKey,
                                value),
                        "Config");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not save metadata metadataStore configuration.", e);
        }
    }

    /**
     * Load configuration of the metadata metadataStore.
     */
    @Override
    public Map<String, String> loadConfiguration() throws SQLException {
        Map<String, String> configuration = new HashMap<>();
        try (ResultSet resultSet = this.databaseAccess.query("SELECT [key], [value] FROM [Config];", "Config")) {
            while (resultSet.next()) {
                configuration.put(resultSet.getString(1), resultSet.getString(2));
            }
        }
        return configuration;
    }

    @Override
    public Int2ObjectMap<String> loadCodes() throws SQLException {
        Int2ObjectMap<String> codes = new Int2ObjectOpenHashMap<>();
        try (ResultSet resultSet = this.databaseAccess.query("SELECT [code], [value] FROM [Code];", "Code")) {
            while (resultSet.next()) {
                codes.put(
                        resultSet.getInt(1),
                        resultSet.getString(2)
                );
            }
        }
        return codes;
    }

    @Override
    public void addCode(int code, String value) {

    }

    @Override
    public void dropTablesIfExist() throws SQLException {
        try (Statement statement = this.databaseAccess.getConnection().createStatement()) {
            // Setting up the schema is not supported by database access. Do it with plain JDBC.
            for (String table : tableNames) {
                String sql = String.format("DROP TABLE IF EXISTS [%s];", table);
                statement.execute(sql);
            }
        }
    }

    /**
     * Flushes any pending inserts/updates to the DB.
     *
     * @throws java.sql.SQLException
     */
    @Override
    public void flush() throws SQLException {
        this.databaseAccess.flush();
    }

    @Override
    public boolean tableExists(String tablename) throws SQLException {
        if (this.existingTables == null) {
            this.loadTableNames();
        }
        // toLowerCase because SQLite is case-insensitive for table names
        return this.existingTables.contains(tablename.toLowerCase());
    }

    @Override
    public void executeCreateTableStatement(String sqlCreateTables) {
        try {
            // Setting up databases is not supported by DatabaseAccess, so we do it directly.
            Connection connection = this.databaseAccess.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(sqlCreateTables);
            }
            if (!this.databaseAccess.getConnection().getAutoCommit()) {
                this.databaseAccess.getConnection().commit();
            }

            this.loadTableNames();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DatabaseAccess getDatabaseAccess() {
        return this.databaseAccess;
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) throws SQLException {
        return this.schemaHandler.getSchemasByName(schemaName);
    }

    @Override
    public void removeSchema(RDBMSSchema schema) throws SQLException {
        this.schemaHandler.removeSchema(schema);
    }

    @Override
    public void removeColumn(RDBMSColumn column) throws SQLException {
        this.schemaHandler.removeColumn(column);
    }

    @Override
    public void removeTable(RDBMSTable table) throws SQLException {
        this.schemaHandler.removeTable(table);
    }

    @Override
    public void removeConstraintCollection(ConstraintCollection<? extends Constraint> constraintCollection) {
        this.constraintHandler.removeConstraintCollection(constraintCollection);
    }


    @Override
    public void setUseJournal(boolean isUseJournal) {
        try {
            this.databaseAccess.flush();
            try (Statement statement = this.databaseAccess.getConnection().createStatement()) {
                statement.execute(String.format("PRAGMA journal_mode = %s;", isUseJournal ? "TRUNCATE" : "OFF"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not change journal usage.", e);
        }
    }

    @Override
    public String toString() {
        return "SQLiteInterface[" + this.databaseAccess.getConnection().getClass() + "]";
    }

    @Override
    public Collection<Experiment> getAllExperimentsForAlgorithm(RDBMSAlgorithm algorithm) {
        return this.experimentHandler.getAllExperimentsForAlgorithm(algorithm);
    }

    @Override
    public void addAlgorithm(RDBMSAlgorithm algorithm) {
        this.experimentHandler.addAlgorithm(algorithm);
    }

    @Override
    public void writeExperiment(RDBMSExperiment experiment) {
        this.experimentHandler.writeExperiment(experiment);
    }

    @Override
    public void addParameterToExperiment(RDBMSExperiment experiment, String key,
                                         String value) {
        this.experimentHandler.addParameterToExperiment(experiment, key, value);

    }

    @Override
    public void setExecutionTimeToExperiment(RDBMSExperiment experiment,
                                             long executionTime) {
        this.experimentHandler.setExecutionTimeToExperiment(experiment, executionTime);

    }

    @Override
    public Set<ConstraintCollection<? extends Constraint>> getAllConstraintCollectionsForExperiment(
            RDBMSExperiment experiment) {
        return this.constraintHandler.getAllConstraintCollectionsForExperiment(experiment);
    }

    @Override
    public Algorithm getAlgorithmByID(int algorithmId) {
        return this.experimentHandler.getAlgorithmFor(algorithmId);
    }

    @Override
    public Experiment getExperimentById(int experimentId) {
        return this.experimentHandler.getExperimentById(experimentId);
    }

    @Override
    public void removeAlgorithm(Algorithm algorithm) {
        this.experimentHandler.removeAlgorithm(algorithm);
    }

    @Override
    public void removeExperiment(Experiment experiment) {
        this.experimentHandler.removeExperiment(experiment);
    }

    @Override
    public Collection<Algorithm> getAllAlgorithms() {
        return this.experimentHandler.getAllAlgorithms();
    }

    @Override
    public Collection<Experiment> getAllExperiments() {
        return this.experimentHandler.getAllExperiments();
    }

    @Override
    public Algorithm getAlgorithmByName(String name) {
        return this.experimentHandler.getAlgorithmByName(name);
    }

    @Override
    public void addAnnotation(RDBMSExperiment experiment, String tag,
                              String text) {
        this.experimentHandler.addAnnotation(experiment, tag, text);

    }

    @Override
    public String getDatabaseURL() {
        try {
            return this.databaseAccess.getConnection().getMetaData().getURL();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;

    }


    @Override
    public void closeMetaDataStore() {
        try {
            this.databaseAccess.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
