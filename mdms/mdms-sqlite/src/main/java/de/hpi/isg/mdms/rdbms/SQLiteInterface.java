package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraint;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.domain.experiment.RDBMSAlgorithm;
import de.hpi.isg.mdms.domain.experiment.RDBMSExperiment;
import de.hpi.isg.mdms.domain.targets.RDBMSColumn;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.domain.targets.RDBMSTable;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * This class acts as an executor of SQLite specific Queries for the {@link de.hpi.isg.mdms.domain.RDBMSMetadataStore}.
 *
 * @author fabian
 */
public class SQLiteInterface implements SQLInterface {

    public static final String[] tableNames = {"Target", "Schemaa", "Tablee", "Columnn", "ConstraintCollection",
            "Constraintt", "Scope", "Location", "LocationProperty", "LocationType", "Config", "Experiment", "Algorithm",
            "ExperimentParameter", "ExperimentException"};

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
     * Keeps track of existing tables within the DB.
     */
    private Set<String> existingTables;

    private int currentConstraintIdMax = -1;

    private SQLiteSchemaHandler schemaHandler;

    private SQLiteConstraintHandler constraintHandler;
    
    private SQLiteExperimentHandler experimentHandler;

    /**
     * Creates a new instance that operates on the given connection.
     *
     * @param connection to operate on
     */
    public SQLiteInterface(Connection connection) {
        this.databaseAccess = new DatabaseAccess(connection);
        this.schemaHandler = new SQLiteSchemaHandler(this.databaseAccess);
        this.constraintHandler = new SQLiteConstraintHandler(this);
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
    public void initializeMetadataStore() {
        // Drop any old tables.
        dropTablesIfExist();

        // Create the DB schema.
        try {
            String sqlCreateTables = loadResource(SETUP_SCRIPT_RESOURCE_PATH);
            this.executeCreateTableStatement(sqlCreateTables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.constraintHandler.initializeTables();

        try {
            flush();
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
    static String loadResource(String resourcePath) throws IOException {
        try (InputStream resourceStream = SQLiteInterface.class.getResourceAsStream(resourcePath)) {
            return IOUtils.toString(resourceStream, "UTF-8");
        }
    }

    /**
     * Loads the tables that exist within the DB.
     */
    private void loadTableNames() {
        existingTables = new HashSet<>();
        DatabaseMetaData meta;
        try {
            meta = this.databaseAccess.getConnection().getMetaData();
            ResultSet res = meta.getTables(null, null, null,
                    new String[]{"TABLE"});
            while (res.next()) {
                // toLowerCase because SQLite is case-insensitive for table names
                existingTables.add(res.getString("TABLE_NAME").toLowerCase());
            }
            res.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean allTablesExist() {
        for (String tableName : tableNames) {
            // toLowerCase because SQLite is case-insensitive for table names
            if (!this.tableExists(tableName.toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addSchema(RDBMSSchema schema) {
        this.schemaHandler.addSchema(schema);
    }

    /**
     * @see SQLiteSchemaHandler#getAllTargets()
     */
    @Override
    public Collection<Target> getAllTargets() {
        return this.schemaHandler.getAllTargets();
    }

    @Override
    public boolean isTargetIdInUse(int id) throws SQLException {
        return this.schemaHandler.isTargetIdInUse(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ConstraintCollection> getAllConstraintCollections() {
        Collection<RDBMSConstraintCollection> constraintCollections = this.constraintHandler.getAllConstraintCollections();
        for (RDBMSConstraintCollection constraintCollection : constraintCollections) {
            Set<Target> scope = getScopeOfConstraintCollection(constraintCollection);
            constraintCollection.setScope(scope);
        }
        return (Collection<ConstraintCollection>) (Collection<?>) constraintCollections;
    }

    @Override
    public void addConstraintCollection(ConstraintCollection constraintCollection) {
        this.constraintHandler.addConstraintCollection(constraintCollection);
    }

    /**
     * @see SQLiteSchemaHandler#getAllSchemas()
     */
    @Override
    public Collection<Schema> getAllSchemas() {
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
    public Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema) {
        return this.schemaHandler.getAllTablesForSchema(rdbmsSchema);
    }

    @Override
    public void addTableToSchema(RDBMSTable newTable, Schema schema) {
        this.schemaHandler.addTableToSchema(newTable, schema);
    }

    @Override
    public Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable) {
        return this.schemaHandler.getAllColumnsForTable(rdbmsTable);
    }

    @Override
    public void addColumnToTable(RDBMSColumn newColumn, Table table) {
        this.schemaHandler.addColumnToTable(newColumn, table);
    }

    @Override
    public void addScope(Target target, ConstraintCollection constraintCollection) {
        this.constraintHandler.addScope(target, constraintCollection);
    }

    @Override
    public Collection<Constraint> getAllConstraintsForConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection) {

        return this.constraintHandler.getAllConstraintsForConstraintCollection(rdbmsConstraintCollection);
    }

    @Override
    public Set<Target> getScopeOfConstraintCollection(RDBMSConstraintCollection rdbmsConstraintCollection) {
        IntCollection targetIds = this.constraintHandler.getScopeOfConstraintCollectionAsIds(rdbmsConstraintCollection);
        Set<Target> scope = new HashSet<>(targetIds.size());
        for (IntIterator i = targetIds.iterator(); i.hasNext(); ) {
            scope.add(this.schemaHandler.loadTarget(i.nextInt()));

        }
        return scope;
    }

    @Override
    public Column getColumnById(int columnId) {
        return this.schemaHandler.getColumnById(columnId);
    }

    @Override
    public Table getTableById(int tableId) {
        return this.schemaHandler.getTableById(tableId);
    }

    @Override
    public Schema getSchemaById(int schemaId) {
        return this.schemaHandler.getSchemaById(schemaId);
    }

    @Override
    public ConstraintCollection getConstraintCollectionById(int id) {
        RDBMSConstraintCollection constraintCollection = this.constraintHandler.getConstraintCollectionById(id);
        if (constraintCollection != null) {
            Set<Target> scope = getScopeOfConstraintCollection(constraintCollection);
            constraintCollection.setScope(scope);
        }
        return constraintCollection;
    }

    @Override
    public void writeConstraint(Constraint constraint) {
        this.constraintHandler.writeConstraint(constraint);
    }

    public void writeConstraint(RDBMSConstraint constraint) {
        this.constraintHandler.writeConstraint(constraint);
    }

    /**
     * Saves configuration of the metadata metadataStore.
     */
    @Override
    public void saveConfiguration() {
        try {
            Map<String, String> configuration = this.store.getConfiguration();
            for (Entry<String, String> configEntry : configuration.entrySet()) {
                String configKey = configEntry.getKey();
                String value = configEntry.getValue();
                this.databaseAccess.executeSQL(
                        String.format("DELETE FROM Config WHERE keyy=\"%s\";", configKey),
                        "Config");
                this.databaseAccess.executeSQL(
                        String.format("INSERT INTO Config (keyy, value) VALUES (\"%s\", \"%s\");",
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
    public Map<String, String> loadConfiguration() {
        Map<String, String> configuration = new HashMap<String, String>();
        try (ResultSet resultSet = this.databaseAccess.query("SELECT keyy, value FROM Config;", "Config")) {
            while (resultSet.next()) {
                configuration.put(resultSet.getString("keyy"), resultSet.getString("value"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not load metadata metadataStore configuration.", e);
        }

        return configuration;
    }

    /**
     * @see SQLiteSchemaHandler#getLocationFor(int)
     */
    @Override
    public Location getLocationFor(int id) {
        return this.schemaHandler.getLocationFor(id);
    }

    @Override
    public void dropTablesIfExist() {
        try {
            // Setting up the schema is not supported by database access. Do it with plain JDBC.
            try (Statement statement = this.databaseAccess.getConnection().createStatement()) {
                for (String table : tableNames) {
                    String sql = String.format("DROP TABLE IF EXISTS [%s];", table);
                    statement.execute(sql);
                }
                this.constraintHandler.dropConstraintTables(statement);
                if (!this.databaseAccess.getConnection().getAutoCommit()) {
                    this.databaseAccess.getConnection().commit();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
    public boolean tableExists(String tablename) {
        if (existingTables == null) {
            loadTableNames();
        }
        // toLowerCase because SQLite is case-insensitive for table names
        return existingTables.contains(tablename.toLowerCase());
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
    public void registerConstraintSQLSerializer(Class<? extends Constraint> clazz,
                                                ConstraintSQLSerializer<? extends Constraint> serializer) {
        this.constraintHandler.registerConstraintSQLSerializer(clazz, serializer);
    }

    @Override
    public DatabaseAccess getDatabaseAccess() {
        return this.databaseAccess;
    }

    @Override
    public Schema getSchemaByName(String schemaName) throws NameAmbigousException {
        return this.schemaHandler.getSchemaByName(schemaName);
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) {
        return this.schemaHandler.getSchemasByName(schemaName);
    }

    @Override
    public Collection<Column> getColumnsByName(String columnName) {
        return this.schemaHandler.getColumnsByName(columnName);
    }

    @Override
    public Column getColumnByName(String columnName, Table table) throws NameAmbigousException {
        return this.schemaHandler.getColumnByName(columnName, table);
    }

    @Override
    public Table getTableByName(String tableName) throws NameAmbigousException {
        return this.schemaHandler.getTableByName(tableName);
    }

    @Override
    public Collection<Table> getTablesByName(String tableName) {
        return this.schemaHandler.getTablesByName(tableName);
    }

    @Override
    public void removeSchema(RDBMSSchema schema) {
        this.schemaHandler.removeSchema(schema);
    }

    @Override
    public void removeColumn(RDBMSColumn column) {
        this.schemaHandler.removeColumn(column);
    }

    @Override
    public void removeTable(RDBMSTable table) {
        this.schemaHandler.removeTable(table);
    }

    @Override
    public void removeConstraintCollection(ConstraintCollection constraintCollection) {
        this.constraintHandler.removeConstraintCollection(constraintCollection);
    }

    /**
     * @see SQLiteSchemaHandler#getLocationClassNames()
     */
    @Override
    public Collection<String> getLocationClassNames() throws SQLException {
        return this.schemaHandler.getLocationClassNames();
    }

    /**
     * @see SQLiteSchemaHandler#getAllSchemas()
     */
    @Override
    public void storeLocationType(Class<? extends Location> locationType) throws SQLException {
        this.schemaHandler.storeLocationType(locationType);
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
	public Set<ConstraintCollection> getAllConstraintCollectionsForExperiment(
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

}
