package de.hpi.isg.metadata_store.domain.factories;

import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;

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
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * This class acts as an executor of SQLite specific Queries for the {@link RDBMSMetadataStore}.
 * 
 * @author fabian
 * @param <RDBMS>
 * 
 */

public class SQLiteInterface implements SQLInterface {

    /**
     * Resource path of the SQL script to set up the metadata store schema.
     */
    private static final String SETUP_SCRIPT_RESOURCE_PATH = "/sqlite/persistence_sqlite.sql";

    public static final String[] tableNames = { "Target", "Schemaa", "Tablee", "Columnn", "ConstraintCollection",
            "Constraintt", "IND", "INDpart", "Scope", "Typee", "Location", "LocationProperty", "Config" };

    private final Map<Class<? extends Constraint>, ConstraintSQLSerializer> constraintSerializers = new HashMap<>();

    private final int CACHE_SIZE = 1000;

    private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_TARGET_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Target (ID, name, locationId) VALUES (?, ?, ?);",
                    new PreparedStatementAdapter<Object[]>() {
                        @Override
                        public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            Target target = (Target) parameters[0];
                            Integer locationId = (Integer) parameters[1];
                            preparedStatement.setInt(1, target.getId());
                            preparedStatement.setString(2, target.getName());
                            preparedStatement.setInt(3, locationId);
                        }
                    },
                    "Target");

    // private static final PreparedStatementBatchWriter.Factory<Target> UPDATE_TARGET_NAME_WRITER_FACTORY =
    // new PreparedStatementBatchWriter.Factory<>(
    // "UPDATE Target set name = ? where id=?;",
    // new PreparedStatementAdapter<Target>() {
    // @Override
    // public void translateParameter(Target target, PreparedStatement preparedStatement) throws SQLException {
    // // TODO generic escapeing
    // preparedStatement.setString(1, target.getName().replace("'", "''"));
    // preparedStatement.setInt(2, target.getId());
    // }
    // },
    // "Target");
    //
    // private static final PreparedStatementBatchWriter.Factory<int[]> UPDATE_TARGET_LOCATION_WRITER_FACTORY =
    // new PreparedStatementBatchWriter.Factory<>(
    // "UPDATE Target set locationId = ? where id=?;",
    // new PreparedStatementAdapter<int[]>() {
    // @Override
    // public void translateParameter(int[] parameters, PreparedStatement preparedStatement) throws SQLException {
    // preparedStatement.setInt(1, parameters[0]);
    // preparedStatement.setInt(2, parameters[1]);
    // }
    // },
    // "Target");

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
                    "SELECT target.id as id, target.name as name,"
                            + " columnn.tableId as tableId"
                            + " from target, columnn"
                            + " where target.id = columnn.id and columnn.id=?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target", "Columnn");

    private static final StrategyBasedPreparedQuery.Factory<Integer> TABLE_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT target.id as id, target.name as name, tablee.schemaId as schemaId"
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
                    "SELECT target.id as id, target.name as name"
                            + " from target, schemaa"
                            + " where target.id = schemaa.id and schemaa.id=?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target", "Schemaa");

    /**
     * @deprecated Use {@link #databaseAccess} instead.
     */
    private final Connection connection;

    private final DatabaseAccess databaseAccess;

    private DatabaseWriter<Object[]> insertTargetWriter;

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

    private DatabaseWriter<Target> updateTargetNameWriter;

    private DatabaseWriter<int[]> updateTargetLocationWriter;

    private DatabaseQuery<Integer> columnQuery;

    private DatabaseQuery<Integer> tableQuery;

    private DatabaseQuery<Integer> schemaQuery;

    private DatabaseQuery<Integer> tableColumnsQuery;

    private DatabaseWriter<Object[]> insertLocationWriter;

    private DatabaseWriter<Object[]> insertLocationPropertyWriter;

    private DatabaseWriter<int[]> insertConstraintWriter;

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
    public void addSchema(Schema schema) {
        try {
            storeTargetWithLocation(schema);
            String sqlSchemaAdd = String.format("INSERT INTO Schemaa (id) VALUES (%d);",
                    schema.getId());
            this.databaseAccess.executeSQL(sqlSchemaAdd, "Schemaa");

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
            Collection<Target> targets = new HashSet<>();
            try (ResultSet rs = this.databaseAccess.query("SELECT id from Target", "Target")) {
                while (rs.next()) {
                    targets.add(buildTarget(rs.getInt("id")));
                }
            }
            allTargets = targets;
            return allTargets;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Target buildTarget(int id) {
        IdUtils idUtils = this.store.getIdUtils();
        if (idUtils.isSchemaId(id)) {
            return getSchemaById(id);
        } else if (idUtils.isTableId(id)) {
            return getTableById(id);
        } else {
            return getColumnById(id);
        }
    }

    @Override
    public Collection<Schema> getAllSchemas() {
        if (allSchemas != null) {
            return allSchemas;
        }
        try {
            Collection<Schema> schemas = new HashSet<>();
            try (ResultSet rs = this.databaseAccess.query(
                    "SELECT Schemaa.id as id, Target.name as name from Schemaa, Target where Target.id = Schemaa.id;",
                    "Schemaa", "Target")) {
                while (rs.next()) {
                    schemas.add(RDBMSSchema.restore(this.store, rs.getInt("id"), rs.getString("name"),
                            getLocationFor(rs.getInt("id"))));
                }
            }

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

    // private boolean addToIdsInUse(int id) {
    // try {
    // this.targetIdWriter.write(id);
    //
    // // invalidate cache
    // allTargets = null;
    //
    // return true;
    // } catch (SQLException e) {
    // throw new RuntimeException(String.format("Could not insert ID %d.", id), e);
    // }
    // }

    private void saveTargetWithLocation(Target target) {
        try {
            // TODO: Merge these two-three queries if possible.
            this.updateTargetNameWriter.write(target);
            Integer locationId = addLocation(target.getLocation());
            this.updateTargetLocationWriter.write(new int[] { locationId, target.getId() });

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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
    public void addConstraint(Constraint constraint) {
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
            constraintSerializers.put(constraint.getClass(), constraint.getConstraintSQLSerializer(this));
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
                    String.format("SELECT id from ConstraintCollection where id=%d;", id);
            try (ResultSet rs = this.databaseAccess.query(getConstraintCollectionByIdQuery, "ConstraintCollection")) {
                while (rs.next()) {
                    constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"), this);
                    constraintCollection.setScope(this.getScopeOfConstraintCollection(constraintCollection));
                    constraintCollection.setConstraints(this
                            .getAllConstraintsOrOfConstraintCollection(constraintCollection));
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
            Collection<ConstraintCollection> constraintCollections = new HashSet<>();
            try (ResultSet rs = this.databaseAccess.query("SELECT id from ConstraintCollection;",
                    "ConstraintCollection")) {
                while (rs.next()) {
                    RDBMSConstraintCollection constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"),
                            this);
                    constraintCollection.setScope(this.getScopeOfConstraintCollection(constraintCollection));
                    constraintCollection.setConstraints(this
                            .getAllConstraintsOrOfConstraintCollection(constraintCollection));
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
            String sqlAddConstraintCollection = String.format("INSERT INTO ConstraintCollection (id) VALUES (%d);",
                    constraintCollection.getId());
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
    public Collection<Table> getAllTablesForSchema(RDBMSSchema rdbmsSchema) {
        try {
            Collection<Table> tables = new HashSet<>();
            String sqlTablesForSchema = String
                    .format("SELECT tablee.id as id, target.name as name from tablee, target where target.id = tablee.id and tablee.schemaId=%d;",
                            rdbmsSchema.getId());
            try (ResultSet rs = this.databaseAccess.query(sqlTablesForSchema, "Target", "Tablee")) {
                while (rs.next()) {
                    RDBMSTable table = RDBMSTable.restore(this.store, rdbmsSchema, rs.getInt("id"),
                            rs.getString("name"),
                            getLocationFor(rs.getInt("id")));
                    this.tableCache.put(table.getId(), table);
                    tables.add(table);
                }
            }
            return tables;
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
            // XXX
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
        // TODO Auto-generated method stub
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
                    .format("SELECT target.id as id, target.name as name"
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
                    .format("SELECT target.id as id, target.name as name"
                            + " from target, schemaa where target.id = schemaa.id and target.name='%s'",
                            schemaName);
            ResultSet rs = databaseAccess.query(sqlSchemaeById, "schemaa", "target");
            while (rs.next()) {
                // second loop
                RDBMSSchema schema = RDBMSSchema.restore(store,
                        rs.getInt("id"),
                        rs.getString("name"),
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
                    .format("SELECT target.id as id, target.name as name, columnn.tableId as tableId"
                            + " from target, columnn where target.id = columnn.id and target.name='%s'",
                            columnName);
            ResultSet rs = databaseAccess.query(sqlColumnsByName, "columnn", "target");
            while (rs.next()) {
                // second loop
                RDBMSColumn column = RDBMSColumn.restore(store,
                        this.getTableById(rs.getInt("tableId")),
                        rs.getInt("id"),
                        rs.getString("name"),
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
    public Column getColumnByName(String columnName) throws NameAmbigousException {
        try {
            String sqlColumnByName = String
                    .format("SELECT target.id as id, target.name as name, columnn.tableId as tableId"
                            + " from target, columnn where target.id = columnn.id and target.name='%s'", columnName
                    );
            ResultSet rs = databaseAccess.query(sqlColumnByName, "columnn", "target");
            RDBMSColumn found = null;
            while (rs.next()) {
                // second loop
                if (found != null) {
                    throw new NameAmbigousException(columnName);
                }
                found = RDBMSColumn.restore(store,
                        this.getTableById(rs.getInt("tableId")),
                        rs.getInt("id"),
                        rs.getString("name"),
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
                    .format("SELECT target.id as id, target.name as name, tablee.schemaId as schemaId"
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
                    .format("SELECT target.id as id, target.name as name, tablee.schemaId as schemaId"
                            + " from target, tablee where target.id = tablee.id and target.name='%s'",
                            tableName);
            ResultSet rs = databaseAccess.query(sqlSchemaeById, "tablee", "target");
            while (rs.next()) {
                // second loop
                RDBMSTable schema = RDBMSTable.restore(store,
                        this.getSchemaById(rs.getInt("schemaId")),
                        rs.getInt("id"),
                        rs.getString("name"),
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
