package de.hpi.isg.metadata_store.domain.factories;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Schema;

/**
 * This class acts as an executor of SQLite specific Queries for the {@link RDBMSMetadataStore}.
 * 
 * @author fabian
 *
 */

public class SQLiteInterface implements SQLInterface {

    Connection connection;

    public SQLiteInterface(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void initializeMetadataStore() {
        try {
            Statement stmt = this.connection.createStatement();
            String sql = "/* Create Tables */\n\nCREATE TABLE [Target]\n(\n\t[id] integer NOT NULL,\n\tPRIMARY KEY ([id])\n);\n\n\nCREATE TABLE [Schemaa]\n(\n\t[name] text,\n\t[id] integer NOT NULL,\n\t[location] text,\n\tPRIMARY KEY ([id]),\n\tFOREIGN KEY ([id])\n\tREFERENCES [Target] ([id])\n);\n\n\nCREATE TABLE [Tablee]\n(\n\t[name] text,\n\t[id] integer NOT NULL,\n\t[schemaId] integer NOT NULL,\n\t[location] text,\n\tPRIMARY KEY ([id]),\n\tFOREIGN KEY ([id])\n\tREFERENCES [Target] ([id]),\n\tFOREIGN KEY ([schemaId])\n\tREFERENCES [Schemaa] ([id])\n);\n\n\nCREATE TABLE [Columnn]\n(\n\t[name] text,\n\t[id] integer NOT NULL,\n\t[tableId] integer NOT NULL,\n\t[locationIndex] integer,\n\tPRIMARY KEY ([id]),\n\tFOREIGN KEY ([tableId])\n\tREFERENCES [Tablee] ([id]),\n\tFOREIGN KEY ([id])\n\tREFERENCES [Target] ([id])\n);\n\n\nCREATE TABLE [ConstraintCollection]\n(\n\t[id] integer NOT NULL,\n\tPRIMARY KEY ([id])\n);\n\n\nCREATE TABLE [Constraintt]\n(\n\t[id] integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n\t[constraintCollectionId] integer NOT NULL,\n\tFOREIGN KEY ([constraintCollectionId])\n\tREFERENCES [ConstraintCollection] ([id])\n);\n\n\nCREATE TABLE [IND]\n(\n\t[constraintId] integer NOT NULL,\n\tPRIMARY KEY ([constraintId]),\n\tFOREIGN KEY ([constraintId])\n\tREFERENCES [Constraintt] ([id])\n);\n\n\nCREATE TABLE [INDpart]\n(\n\t[constraintId] integer NOT NULL,\n\t[lhs] integer NOT NULL,\n\t[rhs] integer NOT NULL,\n\tFOREIGN KEY ([rhs])\n\tREFERENCES [Columnn] ([id]),\n\tFOREIGN KEY ([lhs])\n\tREFERENCES [Columnn] ([id]),\n\tFOREIGN KEY ([constraintId])\n\tREFERENCES [IND] ([constraintId])\n);\n\n\nCREATE TABLE [Scope]\n(\n\t[targetId] integer NOT NULL,\n\t[constraintCollectionId] integer NOT NULL,\n\tFOREIGN KEY ([constraintCollectionId])\n\tREFERENCES [ConstraintCollection] ([id]),\n\tFOREIGN KEY ([targetId])\n\tREFERENCES [Target] ([id])\n);\n\n\nCREATE TABLE [TYPEE]\n(\n\t[typee] text,\n\t[columnId] integer NOT NULL,\n\t[constraintId] integer NOT NULL,\n\tFOREIGN KEY ([columnId])\n\tREFERENCES [Columnn] ([id]),\n\tFOREIGN KEY ([constraintId])\n\tREFERENCES [Constraintt] ([id])\n);";

            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean cotainsTarget(Target target) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void addConstraint(Constraint constraint) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addSchema(Schema schema) {
        // TODO Auto-generated method stub

    }

    @Override
    public Collection<? extends Target> getAllTargets() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<? extends Constraint> getAllConstraints() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<Schema> getAllSchemas() {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub

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

}
