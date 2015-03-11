package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.targets.RDBMSColumn;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.domain.targets.RDBMSTable;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.rdbms.util.LocationCache;
import de.hpi.isg.mdms.util.LRUCache;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * This class manages the serialization and deserialization of schema elements within a SQLiteDatabase.
 *
 * @author sebastian.kruse
 * @since 10.03.2015
 */
public class SQLiteSchemaHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SQLiteSchemaHandler.class);

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

    private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_TARGET_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE FROM Target where id=?;",
                    new PreparedStatementAdapter<Integer>() {
                        @Override
                        public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter);
                        }
                    },
                    "Target");

    private static final PreparedStatementBatchWriter.Factory<Integer[]> INSERT_LOCATION_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Location (id, typee) VALUES (?, ?);",
                    new PreparedStatementAdapter<Integer[]>() {
                        @Override
                        public void translateParameter(Integer[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters[0]);
                            preparedStatement.setInt(2, parameters[1]);
                        }
                    },
                    "Location");

    private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_LOCATION_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE FROM Location where id=?;",
                    new PreparedStatementAdapter<Integer>() {
                        @Override
                        public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter);
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

    private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_LOCATION_PROPERTY_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE FROM LocationProperty where locationId=?;",
                    new PreparedStatementAdapter<Integer>() {
                        @Override
                        public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter);
                        }
                    },
                    "LocationProperty");


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

    private static final PreparedStatementBatchWriter.Factory<RDBMSSchema> DELETE_SCHEMA_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE from Schemaa where id=?",
                    new PreparedStatementAdapter<RDBMSSchema>() {
                        @Override
                        public void translateParameter(RDBMSSchema parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter.getId());
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

    private static final PreparedStatementBatchWriter.Factory<RDBMSTable> DELETE_TABLE_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE from Tablee where id=?",
                    new PreparedStatementAdapter<RDBMSTable>() {
                        @Override
                        public void translateParameter(RDBMSTable parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter.getId());
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

    private static final PreparedStatementBatchWriter.Factory<RDBMSColumn> DELETE_COLUMN_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE from Columnn where id=?;",
                    new PreparedStatementAdapter<RDBMSColumn>() {
                        @Override
                        public void translateParameter(RDBMSColumn parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters.getId());
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
                            + "from LocationProperty "
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

    private final static int CACHE_SIZE = 1000;

    // TODO: Check whether we need these caches? The RDBMSTargets have caches themselves...
    LRUCache<Integer, RDBMSColumn> columnCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Integer, RDBMSTable> tableCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Integer, RDBMSSchema> schemaCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Integer, Location> locationCache = new LRUCache<>(CACHE_SIZE);

    LRUCache<Table, Collection<Column>> allColumnsForTableCache = new LRUCache<>(CACHE_SIZE);

    Collection<Target> allTargets = null;

    Collection<Schema> allSchemas = null;

    /**
     * Encapsulates the access to the database {@link java.sql.Connection}.
     */
    private DatabaseAccess databaseAccess;

    /**
     * The {@link de.hpi.isg.mdms.domain.RDBMSMetadataStore} for that this class manages the schema elements.
     */
    private RDBMSMetadataStore metadataStore;

    /**
     * Helper variable to assign unique DB IDs to {@link de.hpi.isg.mdms.model.location.Location} objects.
     */
    private int currentLocationIdMax = -1;

    private DatabaseQuery<Integer> locationQuery;

    // TODO remove???
    private DatabaseWriter<Target> updateTargetNameWriter;

    private DatabaseWriter<int[]> updateTargetLocationWriter;

    private DatabaseQuery<Integer> tableColumnsQuery;

    private DatabaseQuery<Integer> columnQuery;

    private DatabaseQuery<Integer> tableQuery;

    private DatabaseQuery<Integer> schemaQuery;

    // TODO change generic type to domain types?!
    private DatabaseWriter<Object[]> insertTargetWriter;

    private DatabaseWriter<Integer> deleteTargetWriter;

    private DatabaseWriter<Integer[]> insertLocationWriter;

    private DatabaseWriter<Integer> deleteLocationWriter;

    private DatabaseWriter<Object[]> insertLocationPropertyWriter;

    private DatabaseWriter<Integer> deleteLocationPropertyWriter;

    private DatabaseWriter<int[]> insertConstraintWriter;

    private DatabaseWriter<Integer> deleteConstraintWriter;

    private DatabaseWriter<RDBMSSchema> insertSchemaWriter;

    private DatabaseWriter<RDBMSSchema> deleteSchemaWriter;

    private DatabaseWriter<RDBMSTable> insertTableWriter;

    private DatabaseWriter<RDBMSTable> deleteTableWriter;

    private DatabaseWriter<RDBMSColumn> insertColumnWriter;

    private DatabaseWriter<RDBMSColumn> deleteColumnWriter;

    private DatabaseQuery<Integer> locationPropertiesQuery;

    /**
     * Creates a new instance without association to a metadata store.
     */
    public SQLiteSchemaHandler(DatabaseAccess databaseAccess) {
        this(null, databaseAccess);
    }

    /**
     * Creates a new instance that is immediately associated with a metadata store.
     *
     * @param metadataStore  is the metadata store to that this instance should be associated
     * @param databaseAccess is the access to the SQLite DB in which the schema elements shall be managed.
     */
    public SQLiteSchemaHandler(RDBMSMetadataStore metadataStore, DatabaseAccess databaseAccess) {
        this.metadataStore = metadataStore;
        this.databaseAccess = databaseAccess;

        // Initialize writers and queries.
        try {
            // Writers
            this.insertTargetWriter = this.databaseAccess.createBatchWriter(INSERT_TARGET_WRITER_FACTORY);
            this.deleteTargetWriter = this.databaseAccess.createBatchWriter(DELETE_TARGET_WRITER_FACTORY);
            // this.updateTargetNameWriter = this.databaseAccess.createBatchWriter(UPDATE_TARGET_NAME_WRITER_FACTORY);
            // this.updateTargetLocationWriter =
            // this.databaseAccess.createBatchWriter(UPDATE_TARGET_LOCATION_WRITER_FACTORY);
            this.insertLocationWriter = this.databaseAccess.createBatchWriter(INSERT_LOCATION_WRITER_FACTORY);
            this.deleteLocationWriter = this.databaseAccess.createBatchWriter(DELETE_LOCATION_WRITER_FACTORY);
            this.insertLocationPropertyWriter = this.databaseAccess
                    .createBatchWriter(INSERT_LOCATION_PROPERTY_WRITER_FACTORY);
            this.deleteLocationPropertyWriter = this.databaseAccess
                    .createBatchWriter(DELETE_LOCATION_PROPERTY_WRITER_FACTORY);
            this.insertColumnWriter = this.databaseAccess.createBatchWriter(INSERT_COLUMN_WRITER_FACTORY);
            this.deleteColumnWriter = this.databaseAccess.createBatchWriter(DELETE_COLUMN_WRITER_FACTORY);
            this.insertTableWriter = this.databaseAccess.createBatchWriter(INSERT_TABLE_WRITER_FACTORY);
            this.deleteTableWriter = this.databaseAccess.createBatchWriter(DELETE_TABLE_WRITER_FACTORY);
            this.insertSchemaWriter = this.databaseAccess.createBatchWriter(INSERT_SCHEMA_WRITER_FACTORY);
            this.deleteSchemaWriter = this.databaseAccess.createBatchWriter(DELETE_SCHEMA_WRITER_FACTORY);

            // Queries
            this.locationQuery = this.databaseAccess.createQuery(LOCATION_QUERY_FACTORY);
            this.locationPropertiesQuery = this.databaseAccess.createQuery(LOCATION_PROPERTIES_QUERY_FACTORY);
            this.columnQuery = this.databaseAccess.createQuery(COLUMN_QUERY_FACTORY);
            this.tableQuery = this.databaseAccess.createQuery(TABLE_QUERY_FACTORY);
            this.schemaQuery = this.databaseAccess.createQuery(SCHEMA_QUERY_FACTORY);
            this.tableColumnsQuery = this.databaseAccess.createQuery(TABLE_COLUMNS_QUERY_FACTORY);
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize writers.", e);
        }
    }


    /**
     * Writes the given schema into the database.
     *
     * @param schema is the schema to be written
     */
    public void addSchema(RDBMSSchema schema) {
        try {
            // Write the target and location.
            storeTargetWithLocation(schema);

            // Write the concrete schema information.
            insertSchemaWriter.write(schema);

            // Update schemas.
            if (this.allSchemas != null) {
                this.allSchemas.add(schema);
            }
            this.schemaCache.put(schema.getId(), schema);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a table to the given schema.
     *
     * @param newTable is the table to add to the schema
     * @param schema   is the schema to that the table should be added
     */
    public void addTableToSchema(RDBMSTable newTable, Schema schema) {
        try {
            storeTargetWithLocation(newTable);
            this.insertTableWriter.write(newTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Adds a column to a table.
     *
     * @param newColumn is the column that should be added
     * @param table     is the table to which the column should be added
     */
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

    /**
     * Writes the given target into the target table and also writes its location.
     *
     * @param target is the target with the location to metadataStore.
     * @throws SQLException
     */
    private void storeTargetWithLocation(Target target) throws SQLException {
        // Store the location of the target.
        Integer locationId = addLocation(target.getLocation());
        this.insertTargetWriter.write(new Object[]{target, locationId});

        // update caches
        if (allTargets != null) {
            this.allTargets.add(target);
        }
        if (target instanceof RDBMSSchema) {

        } else if (target instanceof RDBMSTable) {
            this.tableCache.put(target.getId(), (RDBMSTable) target);
        } else if (target instanceof RDBMSColumn) {
            this.columnCache.put(target.getId(), (RDBMSColumn) target);
        }
    }

    /**
     * Stores the given location.
     *
     * @param location is the location to metadataStore
     * @return the DB ID of the added location tuple
     * @throws java.sql.SQLException
     */
    private synchronized Integer addLocation(Location location) throws SQLException {
        if (location == null) {
            return null;
        }

        // for auto-increment id
        ensureCurrentLocationIdMaxInitialized();
        Integer locationId = ++currentLocationIdMax;
        this.insertLocationWriter.write(new Integer[]{locationId, location.getClass().getName().hashCode()});
        for (Map.Entry<String, String> entry : location.getProperties().entrySet()) {
            this.insertLocationPropertyWriter.write(new Object[]{locationId, entry.getKey(), entry.getValue()});
        }
        return locationId;
    }

    /**
     * Checks if {@link #currentLocationIdMax} already has a valid value. If not, a valid value is set.
     */
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

    /**
     * Returns all {@link de.hpi.isg.mdms.model.targets.Target}s from the underlying database.
     *
     * @return the targets
     */
    public Collection<Target> getAllTargets() {
        // If we cached the targets, we can return them directly.
        if (allTargets != null) {
            return allTargets;
        }

        // Otherwise, load all the targets.
        try {
            Int2ObjectMap<RDBMSSchema> schemas = loadAllSchemas();
            Int2ObjectMap<RDBMSTable> tables = loadAllTables(schemas, true);
            Int2ObjectMap<RDBMSColumn> columns = loadAllColumns(tables, null);

            Collection<Target> allTargets = new HashSet<>();
            allTargets.addAll(schemas.values());
            allTargets.addAll(tables.values());
            allTargets.addAll(columns.values());

            // Cache all these targets.
            return this.allTargets = allTargets;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stores a certain subtype of a {@link de.hpi.isg.mdms.model.location.Location} so that it is known to other
     * applications working with the managed metadata metadataStore.
     *
     * @param locationType is a subclass of {@link de.hpi.isg.mdms.model.location.Location}
     * @throws SQLException
     */
    public void storeLocationType(Class<? extends Location> locationType) throws SQLException {
        String sql = String.format("INSERT INTO LocationType (id, className) VALUES (%d, '%s');",
                LocationCache.computeId(locationType),
                locationType.getCanonicalName());
        this.databaseAccess.executeSQL(sql, "LocationType");
    }

    /**
     * Load all schemas from the database (excluding tables and columns, including locations).
     *
     * @return the loaded schemas.
     * @throws SQLException
     */
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
                    Integer locationClassHash = rs.getInt("locationType");
                    Location location = null;
                    if (locationClassHash != null) {
                        location = this.metadataStore.getLocationCache().createLocation(locationClassHash,
                                Collections.<String, String>emptyMap());
                    }
                    schema = RDBMSSchema.restore(this.metadataStore, targetId, name, description, location);
                    schemas.put(targetId, schema);
                    lastSchema = schema;
                }

                // Update location properties for the current schema.
                String locationPropKey = rs.getString("locationPropKey");
                if (locationPropKey != null) {
                    String locationPropVal = rs.getString("locationPropVal");
                    this.metadataStore.getLocationCache().setCanonicalProperty(locationPropKey, locationPropVal,
                            schema.getLocation());
                }
            }
        }

        return schemas;
    }

    /**
     * Loads all tables (for the given schemas, excluding columns, including locations).
     *
     * @param schemas            are the schemas to load the tables for. If no schemas are given, all tables will be loaded.
     * @param areAllSchemasGiven tells if the given schemas are all schemas in the metadata metadataStore
     * @return the loaded tables
     * @throws java.sql.SQLException
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
        IdUtils idUtils = this.metadataStore.getIdUtils();

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

                    Integer locationClassHash = rs.getInt("locationType");
                    Location location = null;
                    if (locationClassHash != null) {
                        location = this.metadataStore.getLocationCache().createLocation(locationClassHash,
                                Collections.<String, String>emptyMap());
                    }

                    int schemaId = idUtils.createGlobalId(idUtils.getLocalSchemaId(targetId));
                    RDBMSSchema schema = schemas.get(schemaId);
                    if (schema == null) {
                        throw new IllegalStateException(String.format("No schema found for table with id %08x.",
                                schemaId));
                    }
                    table = RDBMSTable.restore(this.metadataStore, schema, targetId, name, description, location);
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
                    this.metadataStore.getLocationCache().setCanonicalProperty(locationPropKey, locationPropVal,
                            table.getLocation());
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
     * @param tables are the parent tables for the loaded columns
     * @param schema can be {@code null} or a concrete schema that restricts the columns to be loaded
     * @return the loaded columns indexed by their ID
     * @throws java.sql.SQLException
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
        IdUtils idUtils = this.metadataStore.getIdUtils();
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
                    Integer locationClassHash = rs.getInt("locationType");
                    Location location = null;
                    if (locationClassHash != null) {
                        location = this.metadataStore.getLocationCache().createLocation(locationClassHash,
                                Collections.<String, String>emptyMap());
                    }

                    int tableId = idUtils.createGlobalId(idUtils.getLocalSchemaId(targetId),
                            idUtils.getLocalTableId(targetId));
                    Table table = tables.get(tableId);
                    if (table == null) {
                        throw new IllegalStateException(String.format("No table found for column with id %d.", tableId));
                    }
                    column = RDBMSColumn.restore(this.metadataStore, table, targetId, name, description, location);
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
                    this.metadataStore.getLocationCache().setCanonicalProperty(locationPropKey, locationPropVal,
                            column.getLocation());
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

    /**
     * Loads a target by its ID.
     *
     * @param id is the ID of the target
     * @return the loaded target
     */
    Target loadTarget(int id) {
        IdUtils idUtils = this.metadataStore.getIdUtils();
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

    /**
     * Loads all location types that are stored within the database.
     *
     * @return the class names of the stored location types
     * @throws SQLException
     * @see #storeLocationType(Class)
     */
    public Collection<String> getLocationClassNames() throws SQLException {
        Collection<String> classNames = new LinkedList<>();
        try (ResultSet resultSet = this.databaseAccess.query("SELECT id, className FROM LocationType;", "LocationType")) {
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String className = resultSet.getString("className");
                int computedId = LocationCache.computeId(className);
                if (id != computedId) {
                    throw new IllegalStateException(String.format("Expected ID %x for %s, found %x.", computedId,
                            className, id));
                }
                classNames.add(className);
            }
        }
        return classNames;
    }

    /**
     * Checks whether there exists a schema element with the given ID.
     *
     * @param id is the ID of the questionnable schema element
     * @return whether the schema element exists
     * @throws SQLException
     */
    public boolean isTargetIdInUse(int id) throws SQLException {
        // Check if the ID is in any of the caches or any of the child caches.
        IdUtils idUtils = this.metadataStore.getIdUtils();
        Integer wrappedId = id;
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
     * Loads all schemas from the database.
     *
     * @return the loaded schemas
     */
    public Collection<Schema> getAllSchemas() {
        // Try to return the schemas through caches.
        if (allSchemas != null) {
            return allSchemas;
        }

        // Otherwise load the schemas.
        try {
            Collection<Schema> schemas = new HashSet<>();
            Int2ObjectMap<RDBMSSchema> loadedSchemas = loadAllSchemas();
            schemas.addAll(loadedSchemas.values());
            // Cache the schemas.
            allSchemas = schemas;
            return allSchemas;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads all tables for a schema.
     *
     * @param rdbmsSchema is the schema whose tables shall be loaded
     * @return the tables of the schema
     */
    @SuppressWarnings("unchecked")
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

    /**
     * Loads all columns for a table.
     *
     * @param rdbmsTable is the table whose columns should be loaded
     * @return the loaded columns
     */
    public Collection<Column> getAllColumnsForTable(RDBMSTable rdbmsTable) {
        Collection<Column> allColumnsForTable = allColumnsForTableCache.get(rdbmsTable);
        if (allColumnsForTable != null) {
            return allColumnsForTable;
        }
        try {
            Collection<Column> columns = new HashSet<>();

            String sqlTablesForSchema = String
                    .format("SELECT columnn.id as id from columnn, target where target.id = columnn.id and columnn.tableId=%d;",
                            rdbmsTable.getId());

            ResultSet rs = databaseAccess.query(sqlTablesForSchema, "columnn", "target");
            while (rs.next()) {
                columns.add(getColumnById(rs.getInt("id")));
            }
            rs.close();
            allColumnsForTableCache.put(rdbmsTable, columns);
            return allColumnsForTableCache.get(rdbmsTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads a column with the given ID.
     *
     * @param columnId is the ID of the column to load
     * @return the loaded column
     */
    public Column getColumnById(int columnId) {
        // Try to find a cached column.
        Column cached = columnCache.get(columnId);
        if (cached != null) {
            return cached;
        }

        // Load the column.
        try (ResultSet rs = this.columnQuery.execute(columnId)) {

            while (rs.next()) {
                columnCache.put(columnId,
                        RDBMSColumn.restore(this.metadataStore,
                                this.getTableById(rs.getInt("tableId")),
                                rs.getInt("id"),
                                rs.getString("name"),
                                rs.getString("description"),
                                getLocationFor(rs.getInt("id"))));
                return columnCache.get(columnId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // In case the result set was empty...
        return null;
    }

    /**
     * Load a table with the given ID.
     *
     * @param tableId is the ID of the table to load
     * @return the loaded table
     */
    public Table getTableById(int tableId) {
        Table cached = tableCache.get(tableId);
        if (cached != null) {
            return cached;
        }

        try (ResultSet rs = this.tableQuery.execute(tableId)) {
            while (rs.next()) {
                tableCache.put(tableId, RDBMSTable
                        .restore(this.metadataStore,
                                this.getSchemaById(rs.getInt("schemaId")),
                                rs.getInt("id"),
                                rs.getString("name"),
                                rs.getString("description"),
                                getLocationFor(rs.getInt("id"))));
                return tableCache.get(tableId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    /**
     * Loads the {@link de.hpi.isg.mdms.model.location.Location} for the schema element with the given ID.
     *
     * @param id is the ID of the schema element for that the location should be loaded
     * @return the loaded location
     */
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
                Integer locationClassHash = rs.getInt("typee");

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
                location = this.metadataStore.getLocationCache().createLocation(locationClassHash, locationProperties);
            }
            rs.close();

            locationCache.put(id, location);
            return locationCache.get(id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load a schema with the given ID.
     *
     * @param schemaId is the ID of the schema to load
     * @return the loaded schema
     */
    public Schema getSchemaById(int schemaId) {
        Schema cached = schemaCache.get(schemaId);
        if (cached != null) {
            return cached;
        }

        try (ResultSet rs = this.schemaQuery.execute(schemaId)) {
            while (rs.next()) {
                schemaCache.put(schemaId,
                        RDBMSSchema.restore(this.metadataStore,
                                rs.getInt("id"),
                                rs.getString("name"),
                                rs.getString("description"),
                                getLocationFor(rs.getInt("id"))));
                return schemaCache.get(schemaId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    /**
     * Tries to load a schema with the given name.
     *
     * @param schemaName is the name of the schema to load
     * @return the loaded schema or {@code null} if there is no such schema
     * @throws NameAmbigousException if there are multiple schemata with the given name
     */
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
                found = RDBMSSchema.restore(this.metadataStore,
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

    /**
     * Loads the schemas with the given name
     *
     * @param schemaName is the name of the schema to be loaded
     * @return the loaded schemas
     */
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
                RDBMSSchema schema = RDBMSSchema.restore(this.metadataStore,
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

    /**
     * Loads the columns with the given name.
     *
     * @param columnName is the name of the columns to load
     * @return the loaded columns
     */
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
                RDBMSColumn column = RDBMSColumn.restore(this.metadataStore,
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

    /**
     * Loads the column with the given name.
     *
     * @param columnName is the name of the column to load
     * @param table      is the table that should contain the column
     * @return the column or {@code null} if no such column exists
     * @throws NameAmbigousException if there is more than one such column within the given table
     */
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
                found = RDBMSColumn.restore(this.metadataStore,
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

    /**
     * Loads the table with the given name.
     *
     * @param tableName is the name of the table to load
     * @return the loaded table or {@code null} if no such table exists
     * @throws NameAmbigousException if there is more than table with the given name
     */
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
                found = RDBMSTable.restore(this.metadataStore,
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

    /**
     * Loads the tables with the given name.
     *
     * @param tableName is the name of the table to be loaded
     * @return the loaded tables
     */
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
                RDBMSTable schema = RDBMSTable.restore(this.metadataStore,
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

    /**
     * Removes a schema from the database.
     *
     * @param schema shall be removed
     */
    public void removeSchema(RDBMSSchema schema) {
        try {
            this.deleteSchemaWriter.write(schema);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.allSchemas.remove(schema);
        this.schemaCache.remove(schema.getId());
        removeTargetWithLocation(schema);
    }

    /**
     * Removes a column from the database.
     *
     * @param column should be removed
     */
    public void removeColumn(RDBMSColumn column) {
        try {
            this.deleteColumnWriter.write(column);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (this.allColumnsForTableCache.get(column.getTable()) != null)
            this.allColumnsForTableCache.get(column.getTable()).remove(column);
        this.columnCache.remove(column.getId());
        removeTargetWithLocation(column);
    }

    /**
     * Removes a table from the database.
     *
     * @param table is the table that should be removed
     */
    public void removeTable(RDBMSTable table) {
        try {
            this.deleteTableWriter.write(table);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.tableCache.remove(table);
        this.allColumnsForTableCache.remove(table);
        removeTargetWithLocation(table);
    }

    /**
     * Removes an entry from the target table. Its referenced location is removed as well.
     *
     * @param target is the target whose entry should be removed
     */
    private void removeTargetWithLocation(Target target) {
        // first delete location and location properties
        try {
            ResultSet rs = this.locationQuery.execute(target.getId());
            while (rs.next()) {
                Integer locationId = rs.getInt("id");
                removeLocation(target, locationId);
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // then remove target
        try {
            this.deleteTargetWriter.write(target.getId());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (this.allTargets != null)
            this.allTargets.remove(target);

    }

    /**
     * Remove the given location.
     *
     * @param target     is the target to which the location is associated
     * @param locationId is the ID of the location to remove
     */
    private void removeLocation(Target target, Integer locationId) {
        try {
            this.deleteLocationPropertyWriter.write(locationId);
            this.deleteLocationWriter.write(locationId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        this.locationCache.remove(target.getLocation());
    }

    public void setMetadataStore(RDBMSMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
}
