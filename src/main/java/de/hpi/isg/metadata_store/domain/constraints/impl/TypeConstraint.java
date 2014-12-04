package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.db.PreparedStatementAdapter;
import de.hpi.isg.metadata_store.db.query.DatabaseQuery;
import de.hpi.isg.metadata_store.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.metadata_store.db.write.DatabaseWriter;
import de.hpi.isg.metadata_store.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * This class is a {@link Constraint} representing the data type of a certain {@link Column}. {@link Column}.
 */
public class TypeConstraint extends AbstractConstraint implements Constraint {

    public enum TYPES {
        STRING, INTEGER, DECIMAL
    };

    public static class TypeConstraintSQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "Typee";

        private final SQLInterface sqlInterface;

        DatabaseWriter<Object[]> insertTypeConstraintWriter;

        DatabaseQuery<Void> queryTypeConstraints;

        DatabaseQuery<Integer> queryTypeConstraintsForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_TYPECONSTRAINT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId, typee, columnId) VALUES (?, ?, ?);",
                        new PreparedStatementAdapter<Object[]>() {
                            @Override
                            public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, (Integer) parameters[0]);
                                preparedStatement.setString(2, String.valueOf(parameters[1]));
                                preparedStatement.setInt(3, (Integer) parameters[2]);
                            }
                        },
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> TYPECONSTRAINT_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, typee.columnId as columnId, typee.typee as typee,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from typee, constraintt where typee.constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> TYPECONSTRAINT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, typee.columnId as columnId, typee.typee as typee,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from typee, constraintt where typee.constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public TypeConstraintSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            if (!allTablesExistChecked) {
                if (!sqlInterface.tableExists(tableName)) {
                    String createTable = "CREATE TABLE [" + tableName + "]\n" +
                            "(\n" +
                            "    [constraintId] integer NOT NULL,\n" +
                            "    [columnId] integer NOT NULL,\n" +
                            "    [typee] text,\n" +
                            "    FOREIGN KEY ([constraintId])\n" +
                            "    REFERENCES [Constraintt] ([id]),\n" +
                            "    FOREIGN KEY ([columnId])\n" +
                            "    REFERENCES [Columnn] ([id])\n" +
                            ");";
                    this.sqlInterface.executeCreateTableStatement(createTable);
                }
                if (sqlInterface.tableExists(tableName)) {
                    this.allTablesExistChecked = true;
                }
            }

            try {
                this.insertTypeConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_TYPECONSTRAINT_WRITER_FACTORY);

                this.queryTypeConstraints = sqlInterface.getDatabaseAccess().createQuery(
                        TYPECONSTRAINT_QUERY_FACTORY);

                this.queryTypeConstraintsForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
                        TYPECONSTRAINT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Integer constraintId, Constraint typeConstraint) {
            Validate.isTrue(typeConstraint instanceof TypeConstraint);
            try {
                insertTypeConstraintWriter.write(new Object[] {
                        constraintId, ((TypeConstraint) typeConstraint).getType().name(), typeConstraint
                                .getTargetReference()
                                .getAllTargets().iterator()
                                .next().getId()
                });
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<Constraint> deserializeConstraintsForConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<Constraint> typeConstraints = new HashSet<>();

            try {
                ResultSet rsTypeConstraints = retrieveConstraintCollection ?
                        queryTypeConstraints.execute(null) : queryTypeConstraintsForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsTypeConstraints.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsTypeConstraints
                                        .getInt("constraintCollectionId"));
                    }
                    typeConstraints
                            .add(TypeConstraint.build(
                                    new SingleTargetReference(this.sqlInterface.getColumnById(rsTypeConstraints
                                            .getInt("columnId"))), constraintCollection,
                                    TYPES.valueOf(rsTypeConstraints.getString("typee"))));
                }
                rsTypeConstraints.close();

                return typeConstraints;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final long serialVersionUID = 3194245498846860560L;

    private final TYPES type;

    private final TargetReference target;

    public static TypeConstraint build(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            TYPES type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, constraintCollection, type);
        return typeConstraint;
    }

    public static TypeConstraint buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            TYPES type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, constraintCollection, type);
        constraintCollection.add(typeConstraint);
        return typeConstraint;
    }

    private TypeConstraint(final SingleTargetReference target, ConstraintCollection constraintCollection,
            TYPES type) {
        super(constraintCollection);
        Validate.isTrue(target.getAllTargets().size() == 1);
        for (final Target t : target.getAllTargets()) {
            if (!(t instanceof Column)) {
                throw new IllegalArgumentException("TypeConstrains can only be defined on Columns. But target was: "
                        + t);
            }
        }
        this.type = type;
        this.target = target;
    }

    @Override
    public String toString() {
        return "TypeConstraint [type=" + type + "]";
    }

    @Override
    public TargetReference getTargetReference() {
        return target;
    }

    public TYPES getType() {
        return type;
    }

    @Override
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new TypeConstraintSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }
}
