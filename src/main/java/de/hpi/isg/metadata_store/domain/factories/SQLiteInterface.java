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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency.Reference;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
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

    Connection connection;
    RDBMSMetadataStore store;

    public SQLiteInterface(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void initializeMetadataStore() {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlCreateTables = "\n" +
                    "/* Drop Tables */\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS [INDpart];\n" +
                    "DROP TABLE IF EXISTS [TYPEE];\n" +
                    "DROP TABLE IF EXISTS [Columnn];\n" +
                    "DROP TABLE IF EXISTS [IND];\n" +
                    "DROP TABLE IF EXISTS [Constraintt];\n" +
                    "DROP TABLE IF EXISTS [Scope];\n" +
                    "DROP TABLE IF EXISTS [ConstraintCollection];\n" +
                    "DROP TABLE IF EXISTS [Tablee];\n" +
                    "DROP TABLE IF EXISTS [Schemaa];\n" +
                    "DROP TABLE IF EXISTS [Target];\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "/* Create Tables */\n" +
                    "\n" +
                    "CREATE TABLE [Target]\n" +
                    "(\n" +
                    "    [id] integer NOT NULL,\n" +
                    "    [name] text,\n" +
                    "    [location] text,\n" +
                    "    PRIMARY KEY ([id])\n" +
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
                    "    [locationIndex] integer,\n" +
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
                    "CREATE TABLE [TYPEE]\n" +
                    "(\n" +
                    "    [typee] text,\n" +
                    "    [columnId] integer NOT NULL,\n" +
                    "    [constraintId] integer NOT NULL,\n" +
                    "    FOREIGN KEY ([constraintId])\n" +
                    "    REFERENCES [Constraintt] ([id]),\n" +
                    "    FOREIGN KEY ([columnId])\n" +
                    "    REFERENCES [Columnn] ([id])\n" +
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Collection<Target> getAllTargets() {
        try {
            Collection<Target> targets = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT id from target");
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

    private Target buildTarget(int id) {
        if (IdUtils.isSchemaId(id)) {
            return getSchemaById(id);
        } else if (IdUtils.isTableId(id)) {
            return getTableById(id);
        } else {
            return getColumnById(id);
        }
    }

    @Override
    public Collection<Schema> getAllSchemas() {
        try {
            Collection<Schema> schemas = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt
                    .executeQuery("SELECT schemaa.id as id, target.name as name, target.location as location from schemaa, target where target.id = schemaa.id;");
            while (rs.next()) {
                schemas.add(RDBMSSchema.build(this.store, rs.getInt("id"), rs.getString("name"),
                        new HDFSLocation(rs.getString("location"))));
            }
            rs.close();
            stmt.close();
            return schemas;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Integer> getIdsInUse() {
        try {
            Collection<Integer> idsInUse = new HashSet<>();
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT id from target");
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
            String sql = "INSERT INTO target (ID) " +
                    "VALUES (" + id + ");";
            stmt.executeUpdate(sql);

            stmt.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addTarget(Target target) {
        try {
            PreparedStatement stmt = this.connection.prepareStatement("UPDATE target set name = ? where id=?;");
            // TODO generic escapeing
            stmt.setString(1, target.getName().replace("'", "''"));
            stmt.setInt(2, target.getId());
            stmt.executeUpdate();

            String sqlUpdateLocation = String.format("UPDATE target set location = '%s' where id=%d;", target
                    .getLocation().getPath(), target.getId());
            Statement stmt2 = this.connection.createStatement();
            stmt2.executeUpdate(sqlUpdateLocation);

            stmt.close();
            stmt2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConstraint(Constraint constraint) {
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
                        constraint.getId(), typeConstraint.getType().name(), constraint.getTargetReference()
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
                        constraint.getId());
                stmt.executeUpdate(sqlAddIND);

                for (int i = 0; i < inclusionDependency.getArity(); i++) {
                    String sqlAddINDpart = String.format(
                            "INSERT INTO INDpart (constraintId, lhs, rhs) VALUES ('%d', %d, %d);",
                            constraint.getId(),
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
        try {
            Statement stmt = this.connection.createStatement();
            String sqlColumnById = String
                    .format("SELECT target.id as id, target.name as name, target.location as location,"
                            + " columnn.tableId as tableId, columnn.locationIndex as locationIndex"
                            + " from target, columnn where target.id = columnn.id and columnn.id=%d",
                            columnId);
            ResultSet rs = stmt.executeQuery(sqlColumnById);
            while (rs.next()) {
                return RDBMSColumn
                        .build(store,
                                this.getTableById(rs.getInt("tableId")),
                                rs.getInt("id"),
                                rs.getString("name"),
                                new IndexedLocation(rs.getInt("locationIndex"), new HDFSLocation(rs
                                        .getString("location"))));
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
        try {
            Statement stmt = this.connection.createStatement();
            String sqlTableById = String
                    .format("SELECT target.id as id, target.name as name, target.location as location, tablee.schemaId as schemaId"
                            + " from target, tablee where target.id = tablee.id and tablee.id=%d",
                            tableId);
            ResultSet rs = stmt.executeQuery(sqlTableById);
            while (rs.next()) {
                return RDBMSTable
                        .build(store,
                                this.getSchemaById(rs.getInt("schemaId")),
                                rs.getInt("id"),
                                rs.getString("name"),
                                new HDFSLocation(rs.getString("location")));
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
        try {
            Statement stmt = this.connection.createStatement();
            String sqlSchemaeById = String
                    .format("SELECT target.id as id, target.name as name, target.location as location"
                            + " from target, schemaa where target.id = schemaa.id and schemaa.id=%d",
                            schemaId);
            ResultSet rs = stmt.executeQuery(sqlSchemaeById);
            while (rs.next()) {
                return RDBMSSchema
                        .build(store,
                                rs.getInt("id"),
                                rs.getString("name"),
                                new HDFSLocation(rs.getString("location")));
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
        try {
            Collection<Table> tables = new HashSet<>();
            Statement stmt = this.connection.createStatement();

            String sqlTablesForSchema = String
                    .format("SELECT tablee.id as id, target.name as name, target.location as location from tablee, target where target.id = tablee.id and tablee.schemaId=%d;",
                            rdbmsSchema.getId());

            ResultSet rs = stmt
                    .executeQuery(sqlTablesForSchema);
            while (rs.next()) {
                tables.add(RDBMSTable.build(this.store, rdbmsSchema, rs.getInt("id"), rs.getString("name"),
                        new HDFSLocation(rs.getString("location"))));
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
            return columns;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addColumnToTable(RDBMSColumn newColumn, Table table) {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlColumnAdd = String.format(
                    "INSERT INTO Columnn (id, tableId, locationIndex) VALUES (%d, %d, %d);",
                    newColumn.getId(), table.getId(), newColumn.getLocation().getIndex());
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
                "INDpart", "Scope", "TYPEE" };
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
}
