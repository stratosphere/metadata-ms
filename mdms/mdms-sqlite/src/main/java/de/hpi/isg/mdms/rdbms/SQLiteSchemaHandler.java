package de.hpi.isg.mdms.rdbms;

import com.twitter.chill.KryoPool;
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
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.util.LRUCache;
import org.apache.commons.lang3.Validate;
import scala.Tuple2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * This class manages the serialization and deserialization of schema elements within a SQLiteDatabase.
 *
 * @author sebastian.kruse
 * @since 10.03.2015
 */
public class SQLiteSchemaHandler {

    private final static int CACHE_SIZE = 1000;

    /**
     * Caches {@link Schema}ta that have been loaded earlier.
     */
    private LRUCache<Integer, RDBMSSchema> schemaCache = new LRUCache<>(CACHE_SIZE);

    private boolean isSchemaCacheComplete = false;

    /**
     * Encapsulates the access to the database {@link java.sql.Connection}.
     */
    private final DatabaseAccess databaseAccess;

    /**
     * The {@link de.hpi.isg.mdms.domain.RDBMSMetadataStore} for that this class manages the schema elements.
     */
    private RDBMSMetadataStore metadataStore;

    private DatabaseWriter<Target> insertTargetWriter;

    private DatabaseWriter<Integer> deleteTargetWriter;

    private DatabaseQuery<Integer> targetByIdQuery, targetByParentQuery, targetByTypeQuery;

    private final DatabaseQuery<Tuple2<Integer, String>> targetByParentAndNameQuery;

    private DatabaseQuery<String> schemaByNameQuery;

    private KryoPool kryoPool;

    /**
     * Creates a new instance without association to a metadata store.
     */
    public SQLiteSchemaHandler(DatabaseAccess databaseAccess, KryoPool kryoPool) {
        this(null, databaseAccess, kryoPool);
    }

    /**
     * Creates a new instance that is immediately associated with a metadata store.
     *
     * @param metadataStore  is the metadata store to that this instance should be associated
     * @param databaseAccess is the access to the SQLite DB in which the schema elements shall be managed.
     * @param kryoPool       to (de-)serialize BLOBs
     */
    public SQLiteSchemaHandler(RDBMSMetadataStore metadataStore, DatabaseAccess databaseAccess, KryoPool kryoPool) {
        this.metadataStore = metadataStore;
        this.databaseAccess = databaseAccess;
        this.kryoPool = kryoPool;

        // Initialize writers and queries.
        try {
            final PreparedStatementBatchWriter.Factory<Target> insertTargetWriterFactory = new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO [Target] ([id], [parent], [type_code], [name], [description], [data]) " +
                            "VALUES (?, ?, ?, ?, ?, ?);",
                    (target, preparedStatement) -> {
                        preparedStatement.setInt(1, target.getId());
                        IdUtils idUtils = this.metadataStore.getIdUtils();
                        switch (idUtils.getIdType(target.getId())) {
                            case COLUMN:
                                preparedStatement.setInt(2, idUtils.getTableId(target.getId()));
                                break;
                            case TABLE:
                                preparedStatement.setInt(2, idUtils.getSchemaId(target.getId()));
                                break;
                            default:
                                preparedStatement.setNull(2, Types.INTEGER);
                        }
                        preparedStatement.setInt(3, target.getType().ordinal());
                        preparedStatement.setString(4, target.getName());
                        preparedStatement.setString(5, target.getDescription());
                        preparedStatement.setBytes(6, this.kryoPool.toBytesWithClass(target.getLocation()));
                    },
                    "Target");
            this.insertTargetWriter = this.databaseAccess.createBatchWriter(insertTargetWriterFactory);
            final PreparedStatementBatchWriter.Factory<Integer> deleteTargetWriterFactory = new PreparedStatementBatchWriter.Factory<>(
                    "DELETE FROM [Target] where [id]=?;",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target");
            this.deleteTargetWriter = this.databaseAccess.createBatchWriter(deleteTargetWriterFactory);
            final StrategyBasedPreparedQuery.Factory<Integer> targetQueryFactory = new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT *"
                            + " from [Target]"
                            + " where [Target].[id] = ?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target");
            this.targetByIdQuery = this.databaseAccess.createQuery(targetQueryFactory);
            final StrategyBasedPreparedQuery.Factory<Integer> parentTargetQueryFactory = new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT *"
                            + " from [Target]"
                            + " where [Target].[parent] = ?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target");
            this.targetByParentQuery = this.databaseAccess.createQuery(parentTargetQueryFactory);
            this.targetByTypeQuery = this.databaseAccess.createQuery(new StrategyBasedPreparedQuery.Factory<Integer>(
                    "select * from [Target] where [type_code]=?",
                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                    "Target"
            ));
            this.targetByParentAndNameQuery = this.databaseAccess.createQuery(new StrategyBasedPreparedQuery.Factory<Tuple2<Integer, String>>(
                    "select * from [Target] where [parent]=? and [name]=?",
                    (params, preparedStatement) -> {
                        if (params._1() == null) {
                            preparedStatement.setNull(1, Types.INTEGER);
                        } else {
                            preparedStatement.setInt(1, params._1());
                        }
                        preparedStatement.setString(2, params._2());
                    },
                    "Target"
            ));
            this.schemaByNameQuery = this.databaseAccess.createQuery(new StrategyBasedPreparedQuery.Factory<>(
                    String.format("select * from [Target] where [type_code]=%d and [name]=?", Target.Type.SCHEMA.ordinal()),
                    PreparedStatementAdapter.SINGLE_STRING_ADAPTER,
                    "Target"
            ));
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize writers.", e);
        }
    }


    /**
     * Writes the given schema into the database.
     *
     * @param schema is the schema to be written
     */
    public void writeSchema(RDBMSSchema schema) throws SQLException {
        // Write the target and location.
        this.writeTarget(schema);

        // Update cache.
        this.schemaCache.put(schema.getId(), schema);
    }

    /**
     * Adds a table to the given schema.
     *
     * @param newTable is the table to add to the schema
     */
    public void writeTable(RDBMSTable newTable) throws SQLException {
        this.writeTarget(newTable);
    }

    /**
     * Adds a column to a table.
     *
     * @param newColumn is the column that should be added
     */
    public void writeColumn(RDBMSColumn newColumn) throws SQLException {
        this.writeTarget(newColumn);
    }

    /**
     * Writes the given target into the target table and also writes its location.
     *
     * @param target is the target with the location to metadataStore.
     * @throws SQLException
     */
    private void writeTarget(Target target) throws SQLException {
        // Perform the write.
        this.insertTargetWriter.write(target);
    }


    /**
     * Loads a target by its ID.
     *
     * @param id is the ID of the target
     * @return the loaded target
     */
    Target getTargetById(int id) throws SQLException {
        switch (this.metadataStore.getIdUtils().getIdType(id)) {
            case SCHEMA:
                return this.getSchemaById(id);
            case TABLE:
                return this.getTableById(id);
            case COLUMN:
                return this.getColumnById(id);
        }
        return null;

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
        if (this.metadataStore.getIdUtils().isSchemaId(id) && this.schemaCache.containsKey(wrappedId)) {
            return true;
        }
        // Issue a query, to find out if the ID is in use.
        try (ResultSet rs = this.targetByIdQuery.execute(wrappedId)) {
            return rs.next();
        }
    }

    /**
     * Loads all schemas from the database.
     *
     * @return the loaded schemas
     */
    public Collection<Schema> getAllSchemas() throws SQLException {
        // Try to return the schemas through caches.
        if (this.isSchemaCacheComplete) {
            return new ArrayList<>(this.schemaCache.values());
        }

        this.schemaCache.setEvictionEnabled(false);
        try (ResultSet rs = this.targetByTypeQuery.execute(Target.Type.SCHEMA.ordinal())) {
            while (rs.next()) {
                int id = rs.getInt(1);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.SCHEMA.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                RDBMSSchema schema = RDBMSSchema.restore(this.metadataStore, id, name, description, location);
                this.schemaCache.put(id, schema);
            }

            this.isSchemaCacheComplete = true;
            return new ArrayList<>(this.schemaCache.values());
        }
    }

    /**
     * Load a schema with the given ID.
     *
     * @param schemaId is the ID of the schema to load
     * @return the loaded schema
     */
    public Schema getSchemaById(int schemaId) throws SQLException {
        Schema cached = this.schemaCache.get(schemaId);
        if (cached != null) {
            return cached;
        }

        try (ResultSet rs = this.targetByIdQuery.execute(schemaId)) {
            if (rs.next()) {
                int id = rs.getInt(1);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.SCHEMA.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                RDBMSSchema schema = RDBMSSchema.restore(this.metadataStore, id, name, description, location);
                this.schemaCache.put(schemaId, schema);
                return schema;
            }
        }

        return null;
    }

    /**
     * Loads the schemas with the given name
     *
     * @param schemaName is the name of the schema to be loaded
     * @return the loaded schemas
     */
    public Collection<Schema> getSchemasByName(String schemaName) throws SQLException {
        Collection<Schema> schemas = new ArrayList<>(1);
        try (ResultSet rs = this.schemaByNameQuery.execute(schemaName)) {
            while (rs.next()) {
                int id = rs.getInt(1);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.SCHEMA.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                RDBMSSchema schema = RDBMSSchema.restore(this.metadataStore, id, name, description, location);
                this.schemaCache.put(id, schema);
                schemas.add(schema);
            }
        }
        return schemas;
    }

    /**
     * Load a table with the given ID.
     *
     * @param tableId is the ID of the schema to load
     * @return the loaded table
     */
    public Table getTableById(int tableId) throws SQLException {
        try (ResultSet rs = this.targetByIdQuery.execute(tableId)) {
            if (rs.next()) {
                int id = rs.getInt(1);
                int parentId = rs.getInt(2);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.TABLE.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                return RDBMSTable.restore(
                        this.metadataStore, this.metadataStore.getSchemaById(parentId), id, name, description, location
                );
            }
        }
        return null;
    }


    /**
     * Loads the tables with the given parent {@link Schema}.
     *
     * @param schema the {@link Schema} in which the {@link Table}s reside
     * @return the loaded {@link Table}s
     */
    public Collection<Table> getTables(Schema schema) throws SQLException {
        Collection<Table> tables = new ArrayList<>();
        try (ResultSet rs = this.targetByParentQuery.execute(schema.getId())) {
            while (rs.next()) {
                int id = rs.getInt(1);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.TABLE.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                RDBMSTable table = RDBMSTable.restore(this.metadataStore, schema, id, name, description, location);
                tables.add(table);
            }
        }
        return tables;
    }

    /**
     * Loads the tables with the given name and parent {@link Schema}.
     *
     * @param tableName is the name of the table to be loaded
     * @param schema    the {@link Schema} in which the {@link Table}s reside
     * @return the loaded {@link Table}s
     */
    public Collection<Table> getTables(String tableName, Schema schema) throws SQLException {
        Collection<Table> tables = new ArrayList<>();
        try (ResultSet rs = this.targetByParentAndNameQuery.execute(new Tuple2<>(schema.getId(), tableName))) {
            while (rs.next()) {
                int id = rs.getInt(1);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.TABLE.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                RDBMSTable table = RDBMSTable.restore(this.metadataStore, schema, id, name, description, location);
                tables.add(table);
            }
        }
        return tables;
    }

    /**
     * Load a {@link Column} with the given ID.
     *
     * @param columnId is the ID of the column to load
     * @return the loaded {@link Column}
     */
    public Column getColumnById(int columnId) throws SQLException {
        try (ResultSet rs = this.targetByIdQuery.execute(columnId)) {
            if (rs.next()) {
                int id = rs.getInt(1);
                int parentId = rs.getInt(2);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.COLUMN.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                return RDBMSColumn.restore(
                        this.metadataStore,
                        (Table) this.metadataStore.getTargetById(parentId),
                        id, name, description, location
                );
            }
        }
        return null;
    }


    /**
     * Loads the {@link Column}s with the parent {@link Table}.
     *
     * @param table the parent {@link Table}
     * @return the loaded {@link Column}s
     */
    public Collection<Column> getColumns(Table table) throws SQLException {
        Collection<Column> columns = new ArrayList<>();
        try (ResultSet rs = this.targetByParentQuery.execute(table.getId())) {
            while (rs.next()) {
                int id = rs.getInt(1);
                int typeCode = rs.getInt(3);
                Validate.isTrue(typeCode == Target.Type.COLUMN.ordinal());
                String name = rs.getString(4);
                String description = rs.getString(5);
                Location location = (Location) this.kryoPool.fromBytes(rs.getBytes(6));
                RDBMSColumn column = RDBMSColumn.restore(this.metadataStore, table, id, name, description, location);
                columns.add(column);
            }
        }
        return columns;
    }

    /**
     * Removes a schema from the database.
     *
     * @param schema shall be removed
     */
    public void removeSchema(RDBMSSchema schema) throws SQLException {
        this.databaseAccess.flush(Collections.singleton("Target"));
        this.deleteTargetWriter.write(schema.getId());
        this.schemaCache.remove(schema.getId());
        this.databaseAccess.flush(Collections.singleton("Target"));
    }

    /**
     * Removes a column from the database.
     *
     * @param column should be removed
     */
    public void removeColumn(RDBMSColumn column) throws SQLException {
        this.databaseAccess.flush(Collections.singleton("Target"));
        this.deleteTargetWriter.write(column.getId());
        this.databaseAccess.flush(Collections.singleton("Target"));
    }

    /**
     * Removes a table from the database.
     *
     * @param table is the table that should be removed
     */
    public void removeTable(RDBMSTable table) throws SQLException {
        this.databaseAccess.flush(Collections.singleton("Target"));
        this.deleteTargetWriter.write(table.getId());
        this.databaseAccess.flush(Collections.singleton("Target"));
    }

    public void setMetadataStore(RDBMSMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
}
