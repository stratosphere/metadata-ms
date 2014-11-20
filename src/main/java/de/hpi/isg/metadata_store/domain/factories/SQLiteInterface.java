package de.hpi.isg.metadata_store.domain.factories;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency.Reference;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSTable;
import de.hpi.isg.metadata_store.domain.util.IdUtils;

/**
 * This class acts as an executor of SQLite specific Queries for the {@link RDBMSMetadataStore}.
 * 
 * @author fabian
 *
 */

public class SQLiteInterface implements SQLInterface {

    private final int CACHE_SIZE = 1000;

    Connection connection;
    RDBMSMetadataStore store;

    LRUCache<Integer, Column> columnCache = new LRUCache<>(CACHE_SIZE);
    LRUCache<Integer, Table> tableCache = new LRUCache<>(CACHE_SIZE);
    LRUCache<Integer, Schema> schemaCache = new LRUCache<>(CACHE_SIZE);
    LRUCache<Integer, Location> locationCache = new LRUCache<>(CACHE_SIZE);
    LRUCache<Table, Collection<Column>> allColumnsForTableCache = new LRUCache<>(CACHE_SIZE);
    Collection<Target> allTargets = null;
    Collection<Schema> allSchemas = null;

    public SQLiteInterface(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void initializeMetadataStore() {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlCreateTables = "\n" +
                    "/* DROP TABLE IF EXISTSs */\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS [INDpart];\n" +
                    "DROP TABLE IF EXISTS [Typee];\n" +
                    "DROP TABLE IF EXISTS [Columnn];\n" +
                    "DROP TABLE IF EXISTS [Scope];\n" +
                    "DROP TABLE IF EXISTS [IND];\n" +
                    "DROP TABLE IF EXISTS [Constraintt];\n" +
                    "DROP TABLE IF EXISTS [ConstraintCollection];\n" +
                    "DROP TABLE IF EXISTS [LocationProperty];\n" +
                    "DROP TABLE IF EXISTS [Tablee];\n" +
                    "DROP TABLE IF EXISTS [Schemaa];\n" +
                    "DROP TABLE IF EXISTS [Target];\n" +
                    "DROP TABLE IF EXISTS [Config];\n" +
                    "DROP TABLE IF EXISTS [Location];\n" +
                    "/* Create Tables */\n" +
                    "\n" +
                    "CREATE TABLE [Location]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n" +
                    "    [typee] text\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Target]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL,\n" +
                    "    [name] text,\n" +
                    "    [locationId] integer,\n" +
                    "    PRIMARY KEY ([id]),\n" +
                    "    FOREIGN KEY ([locationId])\n" +
                    "    REFERENCES [Location] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Schemaa]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL,\n" +
                    "    PRIMARY KEY ([id]),\n" +
                    "    FOREIGN KEY ([id])\n" +
                    "    REFERENCES [Target] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Tablee]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL,\n" +
                    "    [schemaId] integer NOT NULL,\n" +
                    "    PRIMARY KEY ([id]),\n" +
                    "    FOREIGN KEY ([id])\n" +
                    "    REFERENCES [Target] ([id]),\n" +
                    "    FOREIGN KEY ([schemaId])\n" +
                    "    REFERENCES [Schemaa] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Columnn]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL,\n" +
                    "    [tableId] integer NOT NULL,\n" +
                    "    PRIMARY KEY ([id]),\n" +
                    "    FOREIGN KEY ([tableId])\n" +
                    "    REFERENCES [Tablee] ([id]),\n" +
                    "    FOREIGN KEY ([id])\n" +
                    "    REFERENCES [Target] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [ConstraintCollection]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL,\n" +
                    "    PRIMARY KEY ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Constraintt]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n" +
                    "    [constraintCollectionId] integer NOT NULL,\n" +
                    "    FOREIGN KEY ([constraintCollectionId])\n" +
                    "    REFERENCES [ConstraintCollection] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [IND]\n" +
                    "(\n" +
                    "    [constraintId] integer NOT NULL,\n" +
                    "    PRIMARY KEY ([constraintId]),\n" +
                    "    FOREIGN KEY ([constraintId])\n" +
                    "    REFERENCES [Constraintt] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [INDpart]\n" +
                    "(\n" +
                    "    [constraintId] integer NOT NULL,\n" +
                    "    [lhs] integer NOT NULL,\n" +
                    "    [rhs] integer NOT NULL,\n" +
                    "    FOREIGN KEY ([lhs])\n" +
                    "    REFERENCES [Columnn] ([id]),\n" +
                    "    FOREIGN KEY ([rhs])\n" +
                    "    REFERENCES [Columnn] ([id]),\n" +
                    "    FOREIGN KEY ([constraintId])\n" +
                    "    REFERENCES [IND] ([constraintId])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [LocationProperty]\n" +
                    "(\n" +
                    "    [locationId] integer NOT NULL,\n" +
                    "    [keyy] text,\n" +
                    "    [value] text,\n" +
                    "    FOREIGN KEY ([locationId])\n" +
                    "    REFERENCES [Location] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Scope]\n" +
                    "(\n" +
                    "    [targetId] integer NOT NULL,\n" +
                    "    [constraintCollectionId] integer NOT NULL,\n" +
                    "    FOREIGN KEY ([constraintCollectionId])\n" +
                    "    REFERENCES [ConstraintCollection] ([id]),\n" +
                    "    FOREIGN KEY ([targetId])\n" +
                    "    REFERENCES [Target] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Typee]\n" +
                    "(\n" +
                    "    [constraintId] integer NOT NULL,\n" +
                    "    [columnId] integer NOT NULL,\n" +
                    "    [typee] text,\n" +
                    "    FOREIGN KEY ([columnId])\n" +
                    "    REFERENCES [Columnn] ([id]),\n" +
                    "    FOREIGN KEY ([constraintId])\n" +
                    "    REFERENCES [Constraintt] ([id])\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "CREATE TABLE [Config]\n" +
                    "(\n" +
                    "    [key] text NOT NULL PRIMARY KEY,\n" +
                    "    [value] text\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "";

            stmt.executeUpdate(sqlCreateTables);
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addSchema(Schema schema) {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlSchemaAdd = String.format("INSERT INTO Schemaa (id) VALUES (%d);",
                    schema.getId());
            stmt.executeUpdate(sqlSchemaAdd);

            stmt.close();

            // invalidate cache
            allSchemas = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Collection<Target> getAllTargets() {
        if (allTargets != null) {
            return allTargets;
        }
        try {
            Collection<Target> targets = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT id from Target");
            while (rs.next()) {
                targets.add(buildTarget(rs.getInt("id")));
            }
            rs.close();
            stmt.close();
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
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt
                    .executeQuery("SELECT Schemaa.id as id, Target.name as name from Schemaa, Target where Target.id = Schemaa.id;");
            while (rs.next()) {
                schemas.add(RDBMSSchema.build(this.store, rs.getInt("id"), rs.getString("name"),
                        getLocationFor(rs.getInt("id"))));
            }
            rs.close();
            stmt.close();
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
            Statement stmt = this.connection.createStatement();
            String locationQuery = String
                    .format("SELECT Location.id as id, Location.typee as typee from Location, Target where Location.id = Target.locationId and Target.id = %d;",
                            id);
            ResultSet rs = stmt
                    .executeQuery(locationQuery);
            while (rs.next()) {
                Class<?> locationClass = Class.forName(rs.getString("typee"));
                if (locationClass.equals(DefaultLocation.class)) {
                    location = new DefaultLocation();

                    Statement stmtProperties = this.connection.createStatement();
                    String locationPropertyQuery = String
                            .format("SELECT LocationProperty.keyy as keyy, LocationProperty.value as value from Location, LocationProperty where LocationProperty.locationId = %s;",
                                    rs.getInt("id"));
                    ResultSet rsProperties = stmtProperties
                            .executeQuery(locationPropertyQuery);
                    while (rsProperties.next()) {
                        location.getProperties().put(rsProperties.getString("keyy"), rsProperties.getString("value"));
                    }
                    rsProperties.close();

                }
            }
            rs.close();
            stmt.close();

            locationCache.put(id, location);
            return locationCache.get(id);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Integer> getIdsInUse() {
        try {
            Collection<Integer> idsInUse = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT id from Target");
            while (rs.next()) {
                idsInUse.add(rs.getInt("id"));
            }
            rs.close();
            stmt.close();
            return idsInUse;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean addToIdsInUse(int id) {
        try {
            Statement stmt = this.connection.createStatement();
            String sql = "INSERT INTO Target (ID) " +
                    "VALUES (" + id + ");";
            stmt.executeUpdate(sql);

            stmt.close();

            // invalidate cache
            allTargets = null;

            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addTarget(Target target) {
        try {
            PreparedStatement stmt = this.connection.prepareStatement("UPDATE Target set name = ? where id=?;");
            // TODO generic escapeing
            stmt.setString(1, target.getName().replace("'", "''"));
            stmt.setInt(2, target.getId());
            stmt.executeUpdate();

            String sqlUpdateLocation = String.format("UPDATE Target set locationId = '%d' where id=%d;",
                    addLocation(target.getLocation()), target.getId());
            Statement stmt2 = this.connection.createStatement();
            stmt2.executeUpdate(sqlUpdateLocation);

            stmt.close();
            stmt2.close();

            // invalidate cache
            allTargets = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized Integer addLocation(Location location) {
        // for auto-increment id
        Integer locationIndex = null;
        try {
            Statement stmt = this.connection.createStatement();
            String sqlAddLocation = String.format(
                    "INSERT INTO Location (typee) VALUES ('%s');",
                    location.getClass().getCanonicalName());
            stmt.executeUpdate(sqlAddLocation);

            String locationId = "select last_insert_rowid() as locationId;";
            ResultSet rsLocationId = stmt.executeQuery(locationId);
            while (rsLocationId.next()) {
                locationIndex = rsLocationId.getInt("locationId");
            }

            for (Entry<String, String> entry : location.getProperties().entrySet()) {
                String sqlAddLocationProperty = String.format(
                        "INSERT INTO LocationProperty (locationId, keyy, value) VALUES (%d, '%s', '%s');",
                        locationIndex,
                        entry.getKey(),
                        entry.getValue());
                stmt.executeUpdate(sqlAddLocationProperty);
            }

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return locationIndex;
    }

    @Override
    public void addConstraint(Constraint constraint) {
        // for auto-increment id
        Integer constraintId = null;
        try {
            Statement stmt = this.connection.createStatement();
            String sqlAddTypeConstraint1;
            if (constraint.getId() == -1) {
                sqlAddTypeConstraint1 = String.format("INSERT INTO Constraintt (constraintCollectionId) VALUES (%d);",
                        constraint.getConstraintCollection().getId());
            } else {
                sqlAddTypeConstraint1 = String.format(
                        "INSERT INTO Constraintt (id, constraintCollectionId) VALUES (%d, %d);",
                        constraint.getId(), constraint.getConstraintCollection().getId());
            }
            stmt.executeUpdate(sqlAddTypeConstraint1);

            if (constraint.getId() == -1) {
                String locationId = "select last_insert_rowid() as constraintId";
                ResultSet rsLocationId = stmt.executeQuery(locationId);
                while (rsLocationId.next()) {
                    constraintId = rsLocationId.getInt("constraintId");
                }
            } else {
                constraintId = constraint.getId();
            }

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (constraint instanceof TypeConstraint) {
            TypeConstraint typeConstraint = (TypeConstraint) constraint;
            try {
                Statement stmt = this.connection.createStatement();
                String sqlAddTypee = String.format(
                        "INSERT INTO Typee (constraintId, typee, columnId) VALUES (%d, '%s', %d);",
                        constraintId, typeConstraint.getType().name(), constraint.getTargetReference()
                                .getAllTargets().iterator()
                                .next().getId());
                stmt.executeUpdate(sqlAddTypee);

                stmt.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (constraint instanceof InclusionDependency) {
            InclusionDependency inclusionDependency = (InclusionDependency) constraint;
            try {
                Statement stmt = this.connection.createStatement();
                String sqlAddIND = String.format(
                        "INSERT INTO IND (constraintId) VALUES (%d);",
                        constraintId);
                stmt.executeUpdate(sqlAddIND);

                for (int i = 0; i < inclusionDependency.getArity(); i++) {
                    String sqlAddINDpart = String.format(
                            "INSERT INTO INDpart (constraintId, lhs, rhs) VALUES ('%d', %d, %d);",
                            constraintId,
                            inclusionDependency.getTargetReference().getDependentColumns()[i].getId(),
                            inclusionDependency.getTargetReference().getReferencedColumns()[i].getId());
                    stmt.executeUpdate(sqlAddINDpart);
                }

                stmt.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Unknown constraint type!");
        }
    }

    @Override
    public Collection<Constraint> getAllConstraintsOrOfConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection) {
        boolean retrieveConstraintCollection = rdbmsConstraintCollection == null;
        String constraintCollectionClause = "";
        if (!retrieveConstraintCollection) {
            constraintCollectionClause = String.format(" and constraintt.constraintCollectionId=%d",
                    rdbmsConstraintCollection.getId());
        }

        Collection<Constraint> constraintsOfCollection = new HashSet<>();
        try {
            // TypeConstraints
            Collection<Constraint> typeConstraints = new HashSet<>();
            Statement stmt = this.connection.createStatement();

            String sqlGetTypeConstraints = String
                    .format("SELECT constraintt.id as id, typee.columnId as columnId, typee.typee as typee,"
                            + " constraintt.constraintCollectionId as constraintCollectionId"
                            + " from typee, constraintt where typee.constraintId = constraintt.id%s;",
                            constraintCollectionClause);
            ResultSet rsTypeConstraints = stmt.executeQuery(sqlGetTypeConstraints);
            while (rsTypeConstraints.next()) {
                if (retrieveConstraintCollection) {
                    rdbmsConstraintCollection = (RDBMSConstraintCollection) this
                            .getConstraintCollectionById(rsTypeConstraints
                                    .getInt("constraintCollectionId"));
                }
                typeConstraints
                        .add(TypeConstraint.build(rsTypeConstraints.getInt("id"),
                                new SingleTargetReference(this.getColumnById(rsTypeConstraints
                                        .getInt("columnId"))), TYPES.valueOf(rsTypeConstraints.getString("typee")),
                                rdbmsConstraintCollection));
                if (retrieveConstraintCollection) {
                    rdbmsConstraintCollection = null;
                }
            }
            rsTypeConstraints.close();
            constraintsOfCollection.addAll(typeConstraints);

            // InclusionDependencies
            Collection<Constraint> inclusionDependencies = new HashSet<>();

            String sqlGetInclusionDependencies = String
                    .format("SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                            + " from IND, constraintt where IND.constraintId = constraintt.id%s;",
                            constraintCollectionClause);
            ResultSet rsInclusionDependencies = stmt.executeQuery(sqlGetInclusionDependencies);
            while (rsInclusionDependencies.next()) {
                if (retrieveConstraintCollection) {
                    rdbmsConstraintCollection = (RDBMSConstraintCollection) this
                            .getConstraintCollectionById(rsTypeConstraints
                                    .getInt("constraintCollectionId"));
                }
                inclusionDependencies
                        .add(InclusionDependency.build(rsInclusionDependencies.getInt("id"),
                                getInclusionDependencyReferences(rsInclusionDependencies.getInt("id")),
                                rdbmsConstraintCollection));
                if (retrieveConstraintCollection) {
                    rdbmsConstraintCollection = null;
                }
            }
            rsInclusionDependencies.close();

            constraintsOfCollection.addAll(inclusionDependencies);

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return constraintsOfCollection;
    }

    @Override
    public Reference getInclusionDependencyReferences(int id) {
        List<Column> lhs = new ArrayList<>();
        List<Column> rhs = new ArrayList<>();
        try {
            Statement stmt = this.connection.createStatement();
            String inclusionDependencyReferenceQuery = String.format(
                    "SELECT lhs, rhs from INDpart where INDpart.constraintId = %d;", id);
            ResultSet rs = stmt
                    .executeQuery(inclusionDependencyReferenceQuery);
            while (rs.next()) {
                lhs.add(this.getColumnById(rs.getInt("lhs")));
                rhs.add(this.getColumnById(rs.getInt("rhs")));
            }
            rs.close();
            stmt.close();
            return new Reference(lhs.toArray(new Column[lhs.size()]), rhs.toArray(new Column[rhs.size()]));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column getColumnById(int columnId) {
        Column cached = columnCache.get(columnId);
        if (cached != null) {
            return cached;
        }
        try {
            Statement stmt = this.connection.createStatement();
            String sqlColumnById = String
                    .format("SELECT target.id as id, target.name as name,"
                            + " columnn.tableId as tableId"
                            + " from target, columnn where target.id = columnn.id and columnn.id=%d",
                            columnId);
            ResultSet rs = stmt.executeQuery(sqlColumnById);
            while (rs.next()) {
                columnCache.put(columnId, RDBMSColumn
                        .build(store,
                                this.getTableById(rs.getInt("tableId")),
                                rs.getInt("id"),
                                rs.getString("name"),
                                getLocationFor(rs.getInt("id"))));
                return columnCache.get(columnId);
            }
            rs.close();
            stmt.close();
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
            Statement stmt = this.connection.createStatement();
            String sqlTableById = String
                    .format("SELECT target.id as id, target.name as name, tablee.schemaId as schemaId"
                            + " from target, tablee where target.id = tablee.id and tablee.id=%d",
                            tableId);
            ResultSet rs = stmt.executeQuery(sqlTableById);
            while (rs.next()) {
                tableCache.put(tableId, RDBMSTable
                        .build(store,
                                this.getSchemaById(rs.getInt("schemaId")),
                                rs.getInt("id"),
                                rs.getString("name"),
                                getLocationFor(rs.getInt("id"))));
                return tableCache.get(tableId);
            }
            rs.close();
            stmt.close();
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
            Statement stmt = this.connection.createStatement();
            String sqlSchemaeById = String
                    .format("SELECT target.id as id, target.name as name"
                            + " from target, schemaa where target.id = schemaa.id and schemaa.id=%d",
                            schemaId);
            ResultSet rs = stmt.executeQuery(sqlSchemaeById);
            while (rs.next()) {
                schemaCache.put(schemaId, RDBMSSchema
                        .build(store,
                                rs.getInt("id"),
                                rs.getString("name"),
                                getLocationFor(rs.getInt("id"))));
                return schemaCache.get(schemaId);
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public ConstraintCollection getConstraintCollectionById(int id) {
        try {
            RDBMSConstraintCollection constraintCollection = null;
            Statement stmt = this.connection.createStatement();
            String getConstraintCollectionByIdQuery = String.format("SELECT id from ConstraintCollection where id=%d;",
                    id);
            ResultSet rs = stmt
                    .executeQuery(getConstraintCollectionByIdQuery);
            while (rs.next()) {
                constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"), this);
                constraintCollection.setScope(this.getScopeOfConstraintCollection(constraintCollection));
                constraintCollection.setConstraints(this
                        .getAllConstraintsOrOfConstraintCollection(constraintCollection));
            }
            rs.close();
            stmt.close();
            return constraintCollection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Target> getScopeOfConstraintCollection(RDBMSConstraintCollection rdbmsConstraintCollection) {
        try {
            Collection<Target> targets = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            String sqlGetScope = String
                    .format("SELECT id from target, scope where scope.targetId = target.id and scope.constraintCollectionId=%d;",
                            rdbmsConstraintCollection.getId());
            ResultSet rs = stmt.executeQuery(sqlGetScope);
            while (rs.next()) {
                targets.add(buildTarget(rs.getInt("id")));
            }
            rs.close();
            stmt.close();
            return targets;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<ConstraintCollection> getAllConstraintCollections() {
        try {
            Collection<ConstraintCollection> constraintCollections = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt
                    .executeQuery("SELECT id from ConstraintCollection;");
            while (rs.next()) {
                RDBMSConstraintCollection constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"), this);
                constraintCollection.setScope(this.getScopeOfConstraintCollection(constraintCollection));
                constraintCollection.setConstraints(this
                        .getAllConstraintsOrOfConstraintCollection(constraintCollection));
                constraintCollections.add(constraintCollection);
            }
            rs.close();
            stmt.close();
            return constraintCollections;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addScope(Target target, ConstraintCollection constraintCollection) {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlAddScope = String.format("INSERT INTO Scope (targetId, constraintCollectionId) VALUES (%d, %d);",
                    target.getId(), constraintCollection.getId());
            stmt.executeUpdate(sqlAddScope);

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConstraintCollection(ConstraintCollection constraintCollection) {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlAddConstraintCollection = String.format("INSERT INTO ConstraintCollection (id) VALUES (%d);",
                    constraintCollection.getId());
            stmt.executeUpdate(sqlAddConstraintCollection);

            stmt.close();
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
        // TODO caching
        try {
            Collection<Table> tables = new HashSet<>();
            Statement stmt = this.connection.createStatement();

            String sqlTablesForSchema = String
                    .format("SELECT tablee.id as id, target.name as name from tablee, target where target.id = tablee.id and tablee.schemaId=%d;",
                            rdbmsSchema.getId());

            ResultSet rs = stmt
                    .executeQuery(sqlTablesForSchema);
            while (rs.next()) {
                tables.add(RDBMSTable.build(this.store, rdbmsSchema, rs.getInt("id"), rs.getString("name"),
                        getLocationFor(rs.getInt("id"))));
            }
            rs.close();
            stmt.close();
            return tables;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addTableToSchema(RDBMSTable newTable, Schema schema) {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlTableAdd = String.format("INSERT INTO Tablee (id, schemaId) VALUES (%d, %d);",
                    newTable.getId(), schema.getId());
            stmt.executeUpdate(sqlTableAdd);

            stmt.close();
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
            Statement stmt = this.connection.createStatement();
            String sqlColumnAdd = String.format(
                    "INSERT INTO Columnn (id, tableId) VALUES (%d, %d);",
                    newColumn.getId(), table.getId());
            stmt.executeUpdate(sqlColumnAdd);

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean tablesExist() {
        DatabaseMetaData meta;
        String[] tableNames = { "Target", "Schemaa", "Tablee", "Columnn", "ConstraintCollection", "Constraintt", "IND",
                "INDpart", "Scope", "Typee", "Location", "LocationProperty", "Config" };
        Set<String> tables = new HashSet<String>(Arrays.asList(tableNames));

        try {
            meta = connection.getMetaData();
            ResultSet res = meta.getTables(null, null, null,
                    new String[] { "TABLE" });
            while (res.next()) {
                tables.remove(res.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (tables.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Saves configuration of the metadata store.
     */
    @Override
    public void saveConfiguration() {
        Map<String, String> configuration = this.store.getConfiguration();
        try (Statement statement = this.connection.createStatement()) {
            for (Map.Entry<String, String> configEntry : configuration.entrySet()) {
                String configKey = configEntry.getKey();
                String value = configEntry.getValue();
                statement.addBatch(String.format("DELETE FROM Config WHERE key=\"%s\";", configKey));
                statement.addBatch(String.format("INSERT INTO Config (key, value) VALUES (\"%s\", \"%s\");", configKey, value));
            }
            statement.executeBatch();
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
        try (Statement statement = this.connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT key, value FROM Config;");
            while (resultSet.next()) {
                configuration.put(resultSet.getString("key"), resultSet.getString("value"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not load metadata store configuration.", e);
        }
        
        return configuration;
    }
}
