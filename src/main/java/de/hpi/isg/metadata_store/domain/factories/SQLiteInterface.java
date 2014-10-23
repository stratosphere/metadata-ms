package de.hpi.isg.metadata_store.domain.factories;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.RDBMSTarget;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSTable;

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
    public void addConstraint(Constraint constraint) {

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
            ResultSet rs = stmt.executeQuery("SELECT id, name, location from target");
            while (rs.next()) {
                targets.add(new RDBMSTarget(this.store, rs.getInt("id"), rs.getString("name"),
                        new HDFSLocation(rs.getString("location"))));
            }
            rs.close();
            stmt.close();
            return targets;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<? extends Constraint> getAllConstraints() {
        return null;
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
            Statement stmt = this.connection.createStatement();
            String sqlUpdateName = String.format("UPDATE target set name = '%s' where id=%d;", target.getName(),
                    target.getId());
            stmt.executeUpdate(sqlUpdateName);

            String sqlUpdateLocation = String.format("UPDATE target set location = '%s' where id=%d;", target
                    .getLocation().getPath(), target.getId());
            stmt.executeUpdate(sqlUpdateLocation);

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<ConstraintCollection> getAllConstraintCollections() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addConstraintCollection(ConstraintCollection constraintCollection) {
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addColumnToTable(RDBMSColumn newColumn, Table table) {
        try {
            Statement stmt = this.connection.createStatement();
            String sqlColumnAdd = String.format("INSERT INTO Columnn (id, tableId) VALUES (%d, %d);",
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
