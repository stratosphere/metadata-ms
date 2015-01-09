package de.hpi.isg.metadata_store.domain.factories;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.db.DatabaseAccess;
import de.hpi.isg.metadata_store.db.PreparedStatementAdapter;
import de.hpi.isg.metadata_store.db.query.DatabaseQuery;
import de.hpi.isg.metadata_store.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.metadata_store.db.write.DatabaseWriter;
import de.hpi.isg.metadata_store.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.constraints.impl.ConstraintSQLSerializer;
import de.hpi.isg.metadata_store.domain.constraints.impl.DistinctValueCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.TupleCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.UniqueColumnCombination;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSTable;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.domain.util.LocationUtils;
import de.hpi.isg.metadata_store.exceptions.ConstraintCollectionEmptyException;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * This class acts as an executor of SQLite specific Queries for the {@link RDBMSMetadataStore}.
 * 
 * @author fabian
 * @param <RDBMS>
 * 
 */

public class SQLiteInterface implements SQLInterface {

    private static final Logger LOG = LoggerFactory.getLogger(SQLInterface.class);
    
    /**
     * Resource path of the SQL script to set up the metadata store schema.
     */
    private static final String SETUP_SCRIPT_RESOURCE_PATH = "/sqlite/persistence_sqlite.sql";

    public static final String[] tableNames = { "Target", "Schemaa", "Tablee", "Columnn", "ConstraintCollection",
            "Constraintt", "Scope", "Location", "LocationProperty", "Config" };

    private final Map<Class<? extends Constraint>, ConstraintSQLSerializer> constraintSerializers = new HashMap<>();

    private final int CACHE_SIZE = 1000;

    private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_TARGET_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Target (ID, name, locationId, description) VALUES (?, ?, ?, ?);",
                    new PreparedStatementAdapter<Object[]>() {
                        @Override
                        public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            Target target = (Target) parameters[0];
                            Integer locationId = (Integer) parameters[1];
                            preparedStatement.setInt(1, target.getId());
                            preparedStatement.setString(2, target.getName());
                            if (locationId == null) {
                                preparedStatement.setNull(3, Types.INTEGER);
                            } else {
                                preparedStatement.setInt(3, locationId);
                            }
                            preparedStatement.setString(4, target.getDescription());
                        }
                    },
                    "Target");

    private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_LOCATION_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Location (id, typee) VALUES (?, ?);",
                    new PreparedStatementAdapter<Object[]>() {
                        @Override
                        public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, (Integer) parameters[0]);
                            preparedStatement.setString(2, (String) parameters[1]);
                        }
                    },
                    "Location");

    private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_LOCATION_PROPERTY_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO LocationProperty (locationId, keyy, value) VALUES (?, ?, ?);",
                    new PreparedStatementAdapter<Object[]>() {
                        @Override
                        public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, (Integer) parameters[0]);
                            preparedStatement.setString(2, (String) parameters[1]);
                            preparedStatement.setString(3, (String) parameters[2]);
                        }
                    },
                    "LocationProperty");

    private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_CONSTRAINT_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Constraintt (id, constraintCollectionId) VALUES (?, ?);",
                    new PreparedStatementAdapter<int[]>() {
                        @Override
                        public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters[0]);
                            preparedStatement.setInt(2, parameters[1]);
                        }
                    },
                    "Constraintt");

    private static final PreparedStatementBatchWriter.Factory<RDBMSSchema> INSERT_SCHEMA_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Schemaa (id) VALUES (?);",
                    new PreparedStatementAdapter<RDBMSSchema>() {
                        @Override
                        public void translateParameter(RDBMSSchema parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters.getId());
                        }
                    },
                    "Schemaa");

    private static final PreparedStatementBatchWriter.Factory<RDBMSTable> INSERT_TABLE_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Tablee (id, schemaId) VALUES (?, ?);",
                    new PreparedStatementAdapter<RDBMSTable>() {
                        @Override
                        public void translateParameter(RDBMSTable parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters.getId());
                            preparedStatement.setInt(2, parameters.getSchema().getId());
                        }
                    },
                    "Tablee");

    private static final PreparedStatementBatchWriter.Factory<RDBMSColumn> INSERT_COLUMN_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Columnn (id, tableId) VALUES (?, ?);",
                    new PreparedStatementAdapter<RDBMSColumn>() {
                        @Override
                        public void translateParameter(RDBMSColumn parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters.getId());
                            preparedStatement.setInt(2, parameters.getTable().getId());
                        }
                    },
                    "Columnn");

    private static final StrategyBasedPreparedQuery.Factory<Integer> LOCATION_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT Location.id as id, Location.typee as typee "
                            + "from Location, Target "
                            + "where Location.id = Target.locationId and Target.id = ?;",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Location", "Target");

    private static final StrategyBasedPreparedQuery.Factory<Integer> LOCATION_PROPERTIES_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT LocationProperty.keyy as keyy, LocationProperty.value as value "
                            + "from Location, LocationProperty "
                            + "where LocationProperty.locationId = ?;",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Location", "LocationProperty");

    private static final StrategyBasedPreparedQuery.Factory<Integer> COLUMN_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT target.id as id, target.name as name, target.description as description,"
                            + " columnn.tableId as tableId"
                            + " from target, columnn"
                            + " where target.id = columnn.id and columnn.id=?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target", "Columnn");

    private static final StrategyBasedPreparedQuery.Factory<Integer> TABLE_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT target.id as id, target.name as name, target.description as description, tablee.schemaId as schemaId"
                            + " from target, tablee"
                            + " where target.id = tablee.id and tablee.id=?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target", "Tablee");

    private static final StrategyBasedPreparedQuery.Factory<Integer> TABLE_COLUMNS_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT columnn.id as id "
                            + "from columnn, target "
                            + "where target.id = columnn.id and columnn.tableId=?;",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target", "Tablee");

    private static final StrategyBasedPreparedQuery.Factory<Integer> SCHEMA_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT target.id as id, target.name as name, target.description as description"
                            + " from target, schemaa"
                            + " where target.id = schemaa.id and schemaa.id=?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target", "Schemaa");

    /**
     * @deprecated Use {@link #databaseAccess} instead.
     */
    private final Connection connection;

    private final DatabaseAccess databaseAccess;

    private int currentConstraintIdMax = -1;

    private int currentLocationIdMax = -1;

    RDBMSMetadataStore store;

    LRUCache<Integer, RDBMSColumn> columnCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Integer, RDBMSTable> tableCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Integer, RDBMSSchema> schemaCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Integer, Location> locationCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Table, Collection<Column>> allColumnsForTableCache = new LRUCache<>(CACHE_SIZE);

    Collection<Target> allTargets = null;

    Collection<Schema> allSchemas = null;

    Set<String> existingTables = null;

    /** @see #LOCATION_QUERY_FACTORY */
    private DatabaseQuery<Integer> locationQuery;

    /** @see #LOCATION_PROPERTIES_QUERY_FACTORY */
    private DatabaseQuery<Integer> locationPropertiesQuery;

    private DatabaseWriter<Object[]> insertTargetWriter;

    private DatabaseWriter<Target> updateTargetNameWriter;

    private DatabaseWriter<int[]> updateTargetLocationWriter;

    private DatabaseQuery<Integer> columnQuery;

    private DatabaseQuery<Integer> tableQuery;

    private DatabaseQuery<Integer> schemaQuery;

    private DatabaseQuery<Integer> tableColumnsQuery;

    private DatabaseWriter<Object[]> insertLocationWriter;

    private DatabaseWriter<Object[]> insertLocationPropertyWriter;

    private DatabaseWriter<int[]> insertConstraintWriter;

    private DatabaseWriter<RDBMSSchema> insertSchemaWriter;

    private DatabaseWriter<RDBMSTable> insertTableWriter;

    private DatabaseWriter<RDBMSColumn> insertColumnWriter;

    public SQLiteInterface(Connection connection) {
        this.connection = connection;
        this.databaseAccess = new DatabaseAccess(connection);

        // Initialize writers and queries.
        try {
            // Writers
            this.insertTargetWriter = this.databaseAccess.createBatchWriter(INSERT_TARGET_WRITER_FACTORY);
            // this.updateTargetNameWriter = this.databaseAccess.createBatchWriter(UPDATE_TARGET_NAME_WRITER_FACTORY);
            // this.updateTargetLocationWriter =
            // this.databaseAccess.createBatchWriter(UPDATE_TARGET_LOCATION_WRITER_FACTORY);
            this.insertLocationWriter = this.databaseAccess.createBatchWriter(INSERT_LOCATION_WRITER_FACTORY);
            this.insertLocationPropertyWriter = this.databaseAccess
                    .createBatchWriter(INSERT_LOCATION_PROPERTY_WRITER_FACTORY);
            this.insertConstraintWriter = this.databaseAccess.createBatchWriter(INSERT_CONSTRAINT_WRITER_FACTORY);
            this.insertColumnWriter = this.databaseAccess.createBatchWriter(INSERT_COLUMN_WRITER_FACTORY);
            this.insertTableWriter = this.databaseAccess.createBatchWriter(INSERT_TABLE_WRITER_FACTORY);
            this.insertSchemaWriter = this.databaseAccess.createBatchWriter(INSERT_SCHEMA_WRITER_FACTORY);

            // Queries
            this.locationQuery = this.databaseAccess.createQuery(LOCATION_QUERY_FACTORY);
            this.locationPropertiesQuery = this.databaseAccess.createQuery(LOCATION_PROPERTIES_QUERY_FACTORY);
            this.columnQuery = this.databaseAccess.createQuery(COLUMN_QUERY_FACTORY);
            this.tableQuery = this.databaseAccess.createQuery(TABLE_QUERY_FACTORY);
            this.schemaQuery = this.databaseAccess.createQuery(SCHEMA_QUERY_FACTORY);
            this.tableColumnsQuery = this.databaseAccess.createQuery(TABLE_COLUMNS_QUERY_FACTORY);
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialze writers.", e);
        }
    }

    public static SQLiteInterface buildAndRegisterStandardConstraints(Connection connection) {
        SQLiteInterface sqlInterface = new SQLiteInterface(connection);
        sqlInterface.registerConstraintSQLSerializer(DistinctValueCount.class,
                new DistinctValueCount.DistinctValueCountSQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(InclusionDependency.class,
                new InclusionDependency.InclusionDependencySQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(TupleCount.class, new TupleCount.TupleCountSQLiteSerializer(
                sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(TypeConstraint.class,
                new TypeConstraint.TypeConstraintSQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(UniqueColumnCombination.class, new
                UniqueColumnCombination.UniqueColumnCombinationSQLiteSerializer(sqlInterface));

        return sqlInterface;
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

                // also drop constraint tables, since they must be invalid after deletion of the store
                for (ConstraintSQLSerializer serializer : this.constraintSerializers.values()) {
                    for (String tableName : serializer.getTableNames()) {
                        // toLowerCase because SQLite is case-insensitive for table names
                        String sql = String.format("DROP TABLE IF EXISTS [%s];", tableName);
                        statement.execute(sql);
                    }
                }
                if (!this.databaseAccess.getConnection().getAutoCommit()) {
                    this.databaseAccess.getConnection().commit();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initializeMetadataStore() {
        dropTablesIfExist();

        try {
            String sqlCreateTables = loadResource(SETUP_SCRIPT_RESOURCE_PATH);
            this.executeCreateTableStatement(sqlCreateTables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // init constraint types
        for (ConstraintSQLSerializer serializer : this.constraintSerializers.values()) {
            serializer.initializeTables();
        }

        try {
            flush();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    /**
     * Loads the given resource as String.
     * 
     * @param resourcePath
     *        is the path of the resource
     * @return a {@link String} with the contents of the resource
     * @throws IOException
     */
    static String loadResource(String resourcePath) throws IOException {
        try (InputStream resourceStream = SQLiteInterface.class.getResourceAsStream(resourcePath)) {
            return IOUtils.toString(resourceStream, "UTF-8");
        }
    }

    @Override
    public void addSchema(RDBMSSchema schema) {
        try {
            storeTargetWithLocation(schema);
            insertSchemaWriter.write(schema);

            // XXX why the flush was necessary
            // this.databaseAccess.flush();

            // TODO: Why not update the schemas directly?
            // invalidate cache

            if (allSchemas != null) {
                allSchemas.add(schema);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void storeTargetWithLocation(Target target) throws SQLException {
        // write target and location to DB
        Integer locationId = addLocation(target.getLocation());
        this.insertTargetWriter.write(new Object[] { target, locationId });

        // update caches
        if (allTargets != null) {
            this.allTargets.add(target);
        }
        if (target instanceof RDBMSSchema) {
            if (this.allSchemas != null) {
                this.allSchemas.add((Schema) target);
            }
            this.schemaCache.put(target.getId(), (RDBMSSchema) target);
        } else if (target instanceof RDBMSTable) {
            this.tableCache.put(target.getId(), (RDBMSTable) target);
        } else if (target instanceof RDBMSColumn) {
            this.columnCache.put(target.getId(), (RDBMSColumn) target);
        }
    }

    @Override
    public Collection<Target> getAllTargets() {
        if (allTargets != null) {
            return allTargets;
        }
        try {
            Int2ObjectMap<RDBMSSchema> schemas = loadAllSchemas();
            Int2ObjectMap<RDBMSTable> tables = loadAllTables(schemas, true);
            Int2ObjectMap<RDBMSColumn> columns = loadAllColumns(tables, null);

            Collection<Target> allTargets = new HashSet<>();
            allTargets.addAll(schemas.values());
            allTargets.addAll(tables.values());
            allTargets.addAll(columns.values());

            return this.allTargets = allTargets;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Int2ObjectMap<RDBMSSchema> loadAllSchemas() throws SQLException {
        String sql = "SELECT target.id AS targetId, target.name AS name, target.description as description, location.typee AS locationType, "
                + "locationproperty.keyy AS locationPropKey, locationproperty.value AS locationPropVal "
                + "FROM schemaa "
                + "JOIN target ON schemaa.id = target.id "
                + "LEFT OUTER JOIN location ON target.locationId = location.id "
                + "LEFT OUTER JOIN locationproperty ON location.id = locationproperty.locationId "
                + "ORDER BY target.id;";

        Int2ObjectMap<RDBMSSchema> schemas = new Int2ObjectOpenHashMap<>();
        RDBMSSchema lastSchema = null;
        // Query schemas together with all important related tables.
        try (ResultSet rs = this.databaseAccess.query(sql, "Schemaa", "Target", "Location", "LocationProperty")) {
            while (rs.next()) {
                // See if we are dealing with the same target as before.
                int targetId = rs.getInt("targetId");
                RDBMSSchema schema = lastSchema;
                if (schema == null || schema.getId() != targetId) {
                    // For a new target, create a new object, potentially with location.
                    String name = rs.getString("name");
                    String description = rs.getString("description");
                    String locationClassName = rs.getString("locationType");
                    Location location = null;
                    if (locationClassName != null) {
                        location = LocationUtils.createLocation(locationClassName,
                                Collections.<String, String> emptyMap());
                    }
                    schema = RDBMSSchema.restore(this.store, targetId, name, description, location);
                    schemas.put(targetId, schema);
                    lastSchema = schema;
                }

                // Update location properties for the current schema.
                String locationPropKey = rs.getString("locationPropKey");
                if (locationPropKey != null) {
                    String locationPropVal = rs.getString("locationPropVal");
                    schema.getLocation().set(locationPropKey, locationPropVal);
                }
            }
        }

        return schemas;
    }

    /**
     * Loads all tables (for the given schemas).
     * 
     * @param schemas
     *        are the schemas to load the tables for
     * @param areAllSchemasGiven
     *        tells if the given schemas are all schemas in the metadata store
     * @return the loaded tables
     * @throws SQLException
     */
    private Int2ObjectMap<RDBMSTable> loadAllTables(Int2ObjectMap<RDBMSSchema> schemas, boolean areAllSchemasGiven)
            throws SQLException {
        
        LOG.trace("Loading all tables for {} schemas.", schemas.size());
        String sql;
        if (areAllSchemasGiven) {
            sql = "SELECT target.id AS targetId, target.name AS name, target.description as description, location.typee AS locationType, "
                    + "locationproperty.keyy AS locationPropKey, locationproperty.value AS locationPropVal "
                    + "FROM tablee "
                    + "JOIN target ON tablee.id = target.id "
                    + "LEFT OUTER JOIN location ON target.locationId = location.id "
                    + "LEFT OUTER JOIN locationproperty ON location.id = locationproperty.locationId "
                    + "ORDER BY target.id;";
        } else {
            sql = "SELECT target.id AS targetId, target.name AS name, target.description as description, location.typee AS locationType, "
                    + "locationproperty.keyy AS locationPropKey, locationproperty.value AS locationPropVal "
                    + "FROM tablee "
                    + "JOIN target ON tablee.id = target.id "
                    + "LEFT OUTER JOIN location ON target.locationId = location.id "
                    + "LEFT OUTER JOIN locationproperty ON location.id = locationproperty.locationId "
                    + "WHERE tablee.schemaId IN (" + StringUtils.join(schemas.keySet(), ",") + ") "
                    + "ORDER BY target.id;";
        }

        Int2ObjectMap<RDBMSTable> tables = new Int2ObjectOpenHashMap<>();
        IdUtils idUtils = this.store.getIdUtils();

        RDBMSTable lastTable = null;
        Int2ObjectOpenHashMap<Collection<Table>> tablesBySchema = new Int2ObjectOpenHashMap<>();

        // Query tables together with all important related tables.
        try (ResultSet rs = this.databaseAccess.query(sql, "Tablee", "Target", "Location", "LocationProperty")) {
            while (rs.next()) {
                // See if we are dealing with the same target as before.
                int targetId = rs.getInt("targetId");
                RDBMSTable table = lastTable;
                if (table == null || table.getId() != targetId) {
                    // For a new target, create a new object, potentially with location.
                    String name = rs.getString("name");
                    String description = rs.getString("description");

                    String locationClassName = rs.getString("locationType");
                    Location location = null;
                    if (locationClassName != null) {
                        location = LocationUtils.createLocation(locationClassName,
                                Collections.<String, String> emptyMap());
                    }

                    int schemaId = idUtils.createGlobalId(idUtils.getLocalSchemaId(targetId));
                    RDBMSSchema schema = schemas.get(schemaId);
                    if (schema == null) {
                        throw new IllegalStateException(String.format("No schema found for table with id %08x.",
                                schemaId));
                    }
                    table = RDBMSTable.restore(this.store, schema, targetId, name, description, location);
                    tables.put(targetId, table);

                    Collection<Table> tablesForSchema = tablesBySchema.get(schemaId);
                    if (tablesForSchema == null) {
                        tablesForSchema = new LinkedList<>();
                        tablesBySchema.put(schemaId, tablesForSchema);
                    }
                    tablesForSchema.add(table);
                    lastTable = table;
                }

                // Update location properties for the current table.
                String locationPropKey = rs.getString("locationPropKey");
                if (locationPropKey != null) {
                    String locationPropVal = rs.getString("locationPropVal");
                    table.getLocation().set(locationPropKey, locationPropVal);
                }
            }

            for (Int2ObjectMap.Entry<Collection<Table>> entry : tablesBySchema.int2ObjectEntrySet()) {
                int schemaId = entry.getIntKey();
                Collection<Table> tablesForSchema = entry.getValue();
                RDBMSSchema rdbmsSchema = schemas.get(schemaId);
                rdbmsSchema.cacheChildTables(tablesForSchema);
            }
        }

        LOG.trace("Loading tables finished.");
        
        return tables;
    }

    /**
     * Loads and caches all columns.
     * 
     * @param tables
     *        are the parent tables for the loaded columns
     * @param schema
     *        can be {@code null} or a concrete schema that restricts the columns to be loaded
     * @return the loaded columns indexed by their ID
     * @throws SQLException
     */
    private Int2ObjectMap<RDBMSColumn> loadAllColumns(Int2ObjectMap<RDBMSTable> tables, RDBMSSchema schema)
            throws SQLException {
        
        LOG.trace("Loading all columns for {} tables.", tables.size());
        
        String sql;
        if (schema == null) {
            sql = "SELECT target.id AS targetId, target.name AS name, target.description as description, location.typee AS locationType, "
                    + "locationproperty.keyy AS locationPropKey, locationproperty.value AS locationPropVal "
                    + "FROM columnn "
                    + "JOIN target ON columnn.id = target.id "
                    + "LEFT OUTER JOIN location ON target.locationId = location.id "
                    + "LEFT OUTER JOIN locationproperty ON location.id = locationproperty.locationId "
                    + "ORDER BY target.id;";
        } else {
            sql = "SELECT target.id AS targetId, target.name AS name, target.description as description, location.typee AS locationType, "
                    + "locationproperty.keyy AS locationPropKey, locationproperty.value AS locationPropVal "
                    + "FROM columnn "
                    + "JOIN tablee ON columnn.tableId = tablee.id " // join also tables
                    + "JOIN target ON columnn.id = target.id "
                    + "LEFT OUTER JOIN location ON target.locationId = location.id "
                    + "LEFT OUTER JOIN locationproperty ON location.id = locationproperty.locationId "
                    + "WHERE tablee.schemaId = " + schema.getId() + " " // and check that they belong to the schema
                    + "ORDER BY target.id;";
        }

        Int2ObjectMap<RDBMSColumn> columns = new Int2ObjectOpenHashMap<>();
        IdUtils idUtils = this.store.getIdUtils();
        Int2ObjectOpenHashMap<Collection<Column>> columnsByTable = new Int2ObjectOpenHashMap<>();

        RDBMSColumn lastColumn = null;
        // Query columns together with all important related columns.
        try (ResultSet rs = this.databaseAccess.query(sql, "Columnn", "Target", "Location", "LocationProperty")) {
            while (rs.next()) {
                // See if we are dealing with the same target as before.
                int targetId = rs.getInt("targetId");
                RDBMSColumn column = lastColumn;
                if (column == null || column.getId() != targetId) {
                    // For a new target, create a new object, potentially with location.
                    String name = rs.getString("name");
                    String description = rs.getString("description");
                    String locationClassName = rs.getString("locationType");
                    Location location = null;
                    if (locationClassName != null) {
                        location = LocationUtils.createLocation(locationClassName,
                                Collections.<String, String> emptyMap());
                    }

                    int tableId = idUtils.createGlobalId(idUtils.getLocalSchemaId(targetId),
                            idUtils.getLocalTableId(targetId));
                    Table table = tables.get(tableId);
                    if (table == null) {
                        throw new IllegalStateException(String.format("No table found for column with id %d.", tableId));
                    }
                    column = RDBMSColumn.restore(this.store, table, targetId, name, description, location);
                    columns.put(targetId, column);

                    Collection<Column> columnsForTable = columnsByTable.get(tableId);
                    if (columnsForTable == null) {
                        columnsForTable = new LinkedList<>();
                        columnsByTable.put(tableId, columnsForTable);
                    }
                    columnsForTable.add(column);
                    lastColumn = column;
                }

                // Update location properties for the current table.
                String locationPropKey = rs.getString("locationPropKey");
                if (locationPropKey != null) {
                    String locationPropVal = rs.getString("locationPropVal");
                    column.getLocation().set(locationPropKey, locationPropVal);
                }
            }

            for (Int2ObjectMap.Entry<Collection<Column>> entry : columnsByTable.int2ObjectEntrySet()) {
                int tableId = entry.getIntKey();
                Collection<Column> columnsForTable = entry.getValue();
                RDBMSTable rdbmsTable = tables.get(tableId);
                rdbmsTable.cacheChildColumns(columnsForTable);
            }
        }

        LOG.trace("Loading columns finished.");
        
        return columns;
    }

    private Target buildTarget(int id) {
        IdUtils idUtils = this.store.getIdUtils();
        switch (idUtils.getIdType(id)) {
        case SCHEMA_ID:
            return getSchemaById(id);
        case TABLE_ID:
            return getTableById(id);
        case COLUMN_ID:
            return getColumnById(id);
        }
        return null;

    }

    @Override
    public Collection<Schema> getAllSchemas() {
        if (allSchemas != null) {
            return allSchemas;
        }
        try {
            Collection<Schema> schemas = new HashSet<>();
            Int2ObjectMap<RDBMSSchema> loadedSchemas = loadAllSchemas();
            schemas.addAll(loadedSchemas.values());
            allSchemas = schemas;
            return allSchemas;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Location getLocationFor(int id) {
        Location cached = locationCache.get(id);
        if (cached != null) {
            return cached;
        }

        try {
            Location location = null;
            ResultSet rs = this.locationQuery.execute(id);
            while (rs.next()) {
                // Get the class name of the location.
                String locationClassName = rs.getString("typee");

                // Load the properties of the location.
                // TODO Rather perform a right outer join with the previous query if too slow.
                Map<String, String> locationProperties = new HashMap<>();
                int locationId = rs.getInt("id");
                ResultSet rsProperties = this.locationPropertiesQuery.execute(locationId);
                while (rsProperties.next()) {
                    locationProperties.put(rsProperties.getString("keyy"), rsProperties.getString("value"));
                }
                rsProperties.close();

                // Create the location.
                location = LocationUtils.createLocation(locationClassName, locationProperties);
            }
            rs.close();

            locationCache.put(id, location);
            return locationCache.get(id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isTargetIdInUse(int id) throws SQLException {
        // Check if the ID is in any of the caches or any of the child caches.
        IdUtils idUtils = this.store.getIdUtils();
        Integer wrappedId = new Integer(id);
        switch (idUtils.getIdType(id)) {
        case SCHEMA_ID:
            if (this.schemaCache.containsKey(wrappedId)) {
                return true;
            }
            break;
        case TABLE_ID:
            if (this.tableCache.containsKey(wrappedId)) {
                return true;
            } else {
                int schemaId = idUtils.createGlobalId(idUtils.getLocalSchemaId(id));
                RDBMSSchema parentSchema = this.schemaCache.get(schemaId);
                if (parentSchema != null) {
                    IntCollection childIdCache = parentSchema.getChildIdCache();
                    if (childIdCache != null) {
                        return childIdCache.contains(id);
                    }
                }
            }
            break;
        case COLUMN_ID:
            if (this.columnCache.containsKey(wrappedId)) {
                return true;
            } else {
                int tableId = idUtils.createGlobalId(idUtils.getLocalSchemaId(id), idUtils.getLocalTableId(id));
                RDBMSTable parentTable = this.tableCache.get(tableId);
                if (parentTable != null) {
                    IntCollection childIdCache = parentTable.getChildIdCache();
                    if (childIdCache != null) {
                        return childIdCache.contains(id);
                    }
                }
            }
        }

        // Issue a query, to find out if the ID is in use.
        boolean isIdInUse = false;
        String sql = String.format("SELECT id FROM Target WHERE id=%d LIMIT 1", id);
        try (ResultSet resultSet = this.databaseAccess.query(sql, "Target")) {
            isIdInUse = resultSet.next();
        }
        return isIdInUse;
    }

    /**
     * Stores the given location.
     * 
     * @param location
     *        is the location to store
     * @return the DB ID of the added location tuple
     * @throws SQLException
     */
    private synchronized Integer addLocation(Location location) throws SQLException {
        if (location == null) {
            return null;
        }

        // for auto-increment id
        ensureCurrentLocationIdMaxInitialized();
        Integer locationId = ++currentLocationIdMax;
        this.insertLocationWriter.write(new Object[] { locationId, location.getClass().getCanonicalName() });
        for (Entry<String, String> entry : location.getProperties().entrySet()) {
            this.insertLocationPropertyWriter.write(new Object[] { locationId, entry.getKey(), entry.getValue() });
        }
        return locationId;
    }

    @Override
    public void writeConstraint(Constraint constraint) {
        ensureCurrentConstraintIdMaxInitialized();

        // for auto-increment id
        Integer constraintId = ++currentConstraintIdMax;
        try {
            this.insertConstraintWriter.write(new int[] { constraintId, constraint.getConstraintCollection().getId() });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        ConstraintSQLSerializer serializer = constraintSerializers.get(constraint.getClass());
        if (serializer == null) {
            serializer = constraint.getConstraintSQLSerializer(this);
            constraintSerializers.put(constraint.getClass(), serializer);
            serializer.initializeTables();

        }

        serializer = constraintSerializers.get(constraint.getClass());

        Validate.isTrue(serializer != null);

        serializer.serialize(constraintId, constraint);
    }

    @Override
    public Collection<Constraint> getAllConstraintsOrOfConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection) {

        Collection<Constraint> constraintsOfCollection = new HashSet<>();

        try {
            this.store.flush();
        } catch (Exception e) {
            throw new RuntimeException("Could not flush metadata store before loading constraints.", e);
        }
        for (ConstraintSQLSerializer constraintSerializer : this.constraintSerializers.values()) {
            constraintsOfCollection.addAll(constraintSerializer
                    .deserializeConstraintsForConstraintCollection(rdbmsConstraintCollection));
        }

        if (constraintsOfCollection.isEmpty()) {
            throw new ConstraintCollectionEmptyException(rdbmsConstraintCollection);
        }

        return constraintsOfCollection;
    }

    @Override
    public Column getColumnById(int columnId) {
        Column cached = columnCache.get(columnId);
        if (cached != null) {
            return cached;
        }
        try {
            try (ResultSet rs = this.columnQuery.execute(columnId)) {

                while (rs.next()) {
                    columnCache.put(columnId,
                            RDBMSColumn.restore(store,
                                    this.getTableById(rs.getInt("tableId")),
                                    rs.getInt("id"),
                                    rs.getString("name"),
                                    rs.getString("description"),
                                    getLocationFor(rs.getInt("id"))));
                    return columnCache.get(columnId);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public Table getTableById(int tableId) {
        Table cached = tableCache.get(tableId);
        if (cached != null) {
            return cached;
        }
        try {
            try (ResultSet rs = this.tableQuery.execute(tableId)) {
                while (rs.next()) {
                    tableCache.put(tableId, RDBMSTable
                            .restore(store,
                                    this.getSchemaById(rs.getInt("schemaId")),
                                    rs.getInt("id"),
                                    rs.getString("name"),
                                    rs.getString("description"),
                                    getLocationFor(rs.getInt("id"))));
                    return tableCache.get(tableId);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public Schema getSchemaById(int schemaId) {
        Schema cached = schemaCache.get(schemaId);
        if (cached != null) {
            return cached;
        }
        try {
            try (ResultSet rs = this.schemaQuery.execute(schemaId)) {
                while (rs.next()) {
                    schemaCache.put(schemaId,
                            RDBMSSchema.restore(store,
                                    rs.getInt("id"),
                                    rs.getString("name"),
                                    rs.getString("description"),
                                    getLocationFor(rs.getInt("id"))));
                    return schemaCache.get(schemaId);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public ConstraintCollection getConstraintCollectionById(int id) {
        try {
            RDBMSConstraintCollection constraintCollection = null;
            String getConstraintCollectionByIdQuery =
                    String.format("SELECT id, description from ConstraintCollection where id=%d;", id);
            try (ResultSet rs = this.databaseAccess.query(getConstraintCollectionByIdQuery, "ConstraintCollection")) {
                while (rs.next()) {
                    constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"), rs.getString("description"),
                            this);
                    constraintCollection.setScope(this.getScopeOfConstraintCollection(constraintCollection));
                }
            }
            return constraintCollection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Target> getScopeOfConstraintCollection(RDBMSConstraintCollection rdbmsConstraintCollection) {
        try {
            Collection<Target> targets = new HashSet<>();
            String sqlGetScope = String
                    .format("SELECT id from target, scope where scope.targetId = target.id and scope.constraintCollectionId=%d;",
                            rdbmsConstraintCollection.getId());
            try (ResultSet rs = this.databaseAccess.query(sqlGetScope, "Target", "Scope")) {
                while (rs.next()) {
                    targets.add(buildTarget(rs.getInt("id")));
                }
            }
            return targets;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<ConstraintCollection> getAllConstraintCollections() {
        try {
            Collection<ConstraintCollection> constraintCollections = new LinkedList<>();
            try (ResultSet rs = this.databaseAccess.query("SELECT id, description from ConstraintCollection;",
                    "ConstraintCollection")) {
                while (rs.next()) {
                    RDBMSConstraintCollection constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"),
                            rs.getString("description"),
                            this);
                    constraintCollection.setScope(this.getScopeOfConstraintCollection(constraintCollection));
                    constraintCollections.add(constraintCollection);
                }
            }
            return constraintCollections;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addScope(Target target, ConstraintCollection constraintCollection) {
        try {
            String sqlAddScope = String.format("INSERT INTO Scope (targetId, constraintCollectionId) VALUES (%d, %d);",
                    target.getId(), constraintCollection.getId());
            this.databaseAccess.executeSQL(sqlAddScope, "Scope");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConstraintCollection(ConstraintCollection constraintCollection) {
        try {
            String sqlAddConstraintCollection = String.format(
                    "INSERT INTO ConstraintCollection (id, description) VALUES (%d, '%s');",
                    constraintCollection.getId(), constraintCollection.getDescription());
            this.databaseAccess.executeSQL(sqlAddConstraintCollection, "ConstraintCollection");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void setMetadataStore(RDBMSMetadataStore rdbmsMetadataStore) {
        this.store = rdbmsMetadataStore;
    }
    
    @Override
    public RDBMSMetadataStore getMetadataStore() {
        return this.store;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema) {
        try {
            Int2ObjectMap<RDBMSSchema> parentSchemas = Int2ObjectMaps.singleton(rdbmsSchema.getId(), rdbmsSchema);
            Int2ObjectMap<RDBMSTable> tables = loadAllTables(parentSchemas, false);
            loadAllColumns(tables, rdbmsSchema);
            return (Collection<Table>) (Collection<?>) tables.values();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addTableToSchema(RDBMSTable newTable, Schema schema) {
        try {
            storeTargetWithLocation(newTable);
            this.insertTableWriter.write(newTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable) {
        Collection<Column> allColumnsForTable = allColumnsForTableCache.get(rdbmsTable);
        if (allColumnsForTable != null) {
            return allColumnsForTable;
        }
        try {
            Collection<Column> columns = new HashSet<>();
            // use DatabaesAccess
            Statement stmt = this.connection.createStatement();

            String sqlTablesForSchema = String
                    .format("SELECT columnn.id as id from columnn, target where target.id = columnn.id and columnn.tableId=%d;",
                            rdbmsTable.getId());

            ResultSet rs = stmt
                    .executeQuery(sqlTablesForSchema);
            while (rs.next()) {
                columns.add(getColumnById(rs.getInt("id")));
            }
            rs.close();
            stmt.close();
            allColumnsForTableCache.put(rdbmsTable, columns);
            return allColumnsForTableCache.get(rdbmsTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addColumnToTable(RDBMSColumn newColumn, Table table) {
        // update cache
        Collection<Column> allColumnsForTable = allColumnsForTableCache.get(table);
        if (allColumnsForTable != null) {
            allColumnsForTable.add(newColumn);
        }

        try {
            storeTargetWithLocation(newColumn);
            this.insertColumnWriter.write(newColumn);
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

    /**
     * Saves configuration of the metadata store.
     */
    @Override
    public void saveConfiguration() {
        try {
            Map<String, String> configuration = this.store.getConfiguration();
            for (Map.Entry<String, String> configEntry : configuration.entrySet()) {
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
            throw new RuntimeException("Could not save metadata store configuration.", e);
        }
    }

    /**
     * Load configuration of the metadata store.
     */
    @Override
    public Map<String, String> loadConfiguration() {
        Map<String, String> configuration = new HashMap<String, String>();
        try (ResultSet resultSet = this.databaseAccess.query("SELECT keyy, value FROM Config;", "Config")) {
            while (resultSet.next()) {
                configuration.put(resultSet.getString("keyy"), resultSet.getString("value"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not load metadata store configuration.", e);
        }

        return configuration;
    }

    /**
     * Flushes any pending inserts/updates to the DB.
     * 
     * @throws SQLException
     */
    @Override
    public void flush() throws SQLException {
        this.databaseAccess.flush();
    }

    @Deprecated
    @Override
    public Statement createStatement() throws SQLException {
        return this.connection.createStatement();
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
    public void registerConstraintSQLSerializer(Class<? extends Constraint> clazz, ConstraintSQLSerializer serializer) {
        constraintSerializers.put(clazz, serializer);
    }

    @Override
    public Schema getSchemaByName(String schemaName) throws NameAmbigousException {
        try {
            String sqlSchemaeById = String
                    .format("SELECT target.id as id, target.name as name, target.description as description"
                            + " from target, schemaa where target.id = schemaa.id and target.name='%s'",
                            schemaName);
            ResultSet rs = databaseAccess.query(sqlSchemaeById, "schemaa", "target");
            RDBMSSchema found = null;
            while (rs.next()) {
                // second loop
                if (found != null) {
                    throw new NameAmbigousException(schemaName);
                }
                found = RDBMSSchema.restore(store,
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        getLocationFor(rs.getInt("id")));

            }
            if (found != null) {
                schemaCache.put(found.getId(), found);
            }

            rs.close();
            return found;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) {
        Collection<Schema> schemas = new HashSet<>();
        try {

            String sqlSchemaeById = String
                    .format("SELECT target.id as id, target.name as name, target.description as description"
                            + " from target, schemaa where target.id = schemaa.id and target.name='%s'",
                            schemaName);
            ResultSet rs = databaseAccess.query(sqlSchemaeById, "schemaa", "target");
            while (rs.next()) {
                // second loop
                RDBMSSchema schema = RDBMSSchema.restore(store,
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        getLocationFor(rs.getInt("id")));
                schemaCache.put(schema.getId(), schema);
                schemas.add(schema);
            }
            rs.close();
            return schemas;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void loadTableNames() {
        existingTables = new HashSet<>();
        DatabaseMetaData meta;
        try {
            meta = this.databaseAccess.getConnection().getMetaData();
            ResultSet res = meta.getTables(null, null, null,
                    new String[] { "TABLE" });
            while (res.next()) {
                // toLowerCase because SQLite is case-insensitive for table names
                existingTables.add(res.getString("TABLE_NAME").toLowerCase());
            }
            res.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureCurrentConstraintIdMaxInitialized() {
        if (this.currentConstraintIdMax != -1) {
            return;
        }
        try {
            this.currentConstraintIdMax = 0;
            try (ResultSet res = this.databaseAccess.query("SELECT MAX(id) from Constraintt;", "Constraintt")) {
                while (res.next()) {
                    this.currentConstraintIdMax = res.getInt("max(id)");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureCurrentLocationIdMaxInitialized() {
        if (this.currentLocationIdMax != -1) {
            return;
        }
        try {
            this.currentLocationIdMax = 0;
            try (ResultSet res = this.databaseAccess.query("SELECT MAX(id) from Location;", "Location")) {
                while (res.next()) {
                    this.currentLocationIdMax = res.getInt("max(id)");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Column> getColumnsByName(String columnName) {
        Collection<Column> columns = new HashSet<>();
        try {

            String sqlColumnsByName = String
                    .format("SELECT target.id as id, target.name as name, target.description as description, columnn.tableId as tableId"
                            + " from target, columnn where target.id = columnn.id and target.name='%s'",
                            columnName);
            ResultSet rs = databaseAccess.query(sqlColumnsByName, "columnn", "target");
            while (rs.next()) {
                // second loop
                RDBMSColumn column = RDBMSColumn.restore(store,
                        this.getTableById(rs.getInt("tableId")),
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        getLocationFor(rs.getInt("id")));
                columnCache.put(column.getId(), column);
                columns.add(column);
            }
            rs.close();
            return columns;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column getColumnByName(String columnName, Table table) throws NameAmbigousException {
        try {
            String sqlColumnByName = String
                    .format("SELECT target.id as id, target.name as name, target.description as description, columnn.tableId as tableId"
                            + " from target, columnn where target.id = columnn.id and target.name='%s'"
                            + " and columnn.tableId=%d", columnName, table.getId()
                    );
            ResultSet rs = databaseAccess.query(sqlColumnByName, "columnn", "target");
            RDBMSColumn found = null;
            while (rs.next()) {
                // second loop
                if (found != null) {
                    throw new NameAmbigousException(columnName);
                }
                found = RDBMSColumn.restore(store,
                        table,
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        getLocationFor(rs.getInt("id")));

            }
            if (found != null) {
                columnCache.put(found.getId(), found);
            }

            rs.close();
            return found;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table getTableByName(String tableName) throws NameAmbigousException {
        try {
            String sqlTableByname = String
                    .format("SELECT target.id as id, target.name as name, target.description as description, tablee.schemaId as schemaId"
                            + " from target, tablee where target.id = tablee.id and target.name='%s'",
                            tableName);
            ResultSet rs = databaseAccess.query(sqlTableByname, "tablee", "target");
            RDBMSTable found = null;
            while (rs.next()) {
                // second loop
                if (found != null) {
                    throw new NameAmbigousException(tableName);
                }
                found = RDBMSTable.restore(store,
                        this.getSchemaById(rs.getInt("schemaId")),
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        getLocationFor(rs.getInt("id")));

            }
            if (found != null) {
                tableCache.put(found.getId(), found);
            }

            rs.close();
            return found;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Table> getTablesByName(String tableName) {
        Collection<Table> tables = new HashSet<>();
        try {

            String sqlSchemaeById = String
                    .format("SELECT target.id as id, target.name as name, target.description as description, tablee.schemaId as schemaId"
                            + " from target, tablee where target.id = tablee.id and target.name='%s'",
                            tableName);
            ResultSet rs = databaseAccess.query(sqlSchemaeById, "tablee", "target");
            while (rs.next()) {
                // second loop
                RDBMSTable schema = RDBMSTable.restore(store,
                        this.getSchemaById(rs.getInt("schemaId")),
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        getLocationFor(rs.getInt("id")));
                tableCache.put(schema.getId(), schema);
                tables.add(schema);
            }
            rs.close();
            return tables;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DatabaseAccess getDatabaseAccess() {
        return this.databaseAccess;
    }
}
