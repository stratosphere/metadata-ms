package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * This class is a {@link de.hpi.isg.mdms.model.constraints.Constraint} representing the data type of a certain {@link Column}. {@link Column}.
 */
public class TypeConstraint extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeConstraint.class);

    public static class TypeConstraintSQLiteSerializer implements ConstraintSQLSerializer<TypeConstraint> {

        private final static String tableName = "Typee";

        private final SQLInterface sqlInterface;

        DatabaseWriter<Object[]> insertTypeConstraintWriter;

        DatabaseWriter<Integer> deleteTypeConstraintWriter;

        DatabaseQuery<Void> queryTypeConstraints;

        DatabaseQuery<Integer> queryTypeConstraintsForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_TYPECONSTRAINT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintCollectionId, typee, columnId) VALUES (?, ?, ?);",
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

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_TYPECONSTRAINT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from " + tableName
                                + " where constraintId=?;",
                        new PreparedStatementAdapter<Integer>() {
                            @Override
                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter);
                            }
                        },
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> TYPECONSTRAINT_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT typee.constraintId as id, typee.columnId as columnId, typee.typee as typee,"
                                + " typee.constraintCollectionId as constraintCollectionId"
                                + " from typee;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> TYPECONSTRAINT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT typee.constraintId as id, typee.columnId as columnId, typee.typee as typee,"
                                + " typee.constraintCollectionId as constraintCollectionId"
                                + " from typee where typee.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public TypeConstraintSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertTypeConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_TYPECONSTRAINT_WRITER_FACTORY);

                this.deleteTypeConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_TYPECONSTRAINT_WRITER_FACTORY);

                this.queryTypeConstraints = sqlInterface.getDatabaseAccess().createQuery(
                        TYPECONSTRAINT_QUERY_FACTORY);

                this.queryTypeConstraintsForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
                        TYPECONSTRAINT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint typeConstraint, ConstraintCollection constraintCollection) {
            Validate.isTrue(typeConstraint instanceof TypeConstraint);
            try {
                insertTypeConstraintWriter.write(new Object[] {
                        constraintCollection.getId(), ((TypeConstraint) typeConstraint).getType(), typeConstraint
                                .getTargetReference()
                                .getAllTargetIds().iterator().nextInt()
                });
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<TypeConstraint> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<TypeConstraint> typeConstraints = new HashSet<>();

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
                            .add(new TypeConstraint(
                                    new SingleTargetReference(this.sqlInterface.getColumnById(rsTypeConstraints
                                            .getInt("columnId")).getId()),
                                    rsTypeConstraints.getString("typee")));
                }
                rsTypeConstraints.close();

                return typeConstraints;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getTableNames() {
            return Arrays.asList(tableName);
        }

        @Override
        public void initializeTables() {
            if (!sqlInterface.tableExists(tableName)) {
                String createTable = "CREATE TABLE [" + tableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [constraintCollectionId] integer NOT NULL,\n" +
                        "    [columnId] integer NOT NULL,\n" +
                        "    [typee] text,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "    FOREIGN KEY ([constraintCollectionId])\n" +
                        "    REFERENCES [ConstraintCollection] ([id]),\n" +
                        "    FOREIGN KEY ([columnId])\n" +
                        "    REFERENCES [Columnn] ([id])\n" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createTable);
            }
            if (!sqlInterface.tableExists(tableName)) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            try {
                ResultSet rsTypeConstraints = queryTypeConstraintsForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsTypeConstraints.next()) {
                    this.deleteTypeConstraintWriter.write(rsTypeConstraints.getInt("id"));
                }
                rsTypeConstraints.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final long serialVersionUID = 3194245498846860560L;

    private final String type;

    private final TargetReference target;

    @Deprecated
    public static TypeConstraint build(final SingleTargetReference target, String type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, type);
        return typeConstraint;
    }

    public static TypeConstraint buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection<TypeConstraint> constraintCollection,
            String type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, type);
        constraintCollection.add(typeConstraint);
        return typeConstraint;
    }

    public TypeConstraint(final SingleTargetReference target, String type) {
        Validate.isTrue(target.getAllTargetIds().size() == 1);

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

    public String getType() {
        return type;
    }

    @Override
    public ConstraintSQLSerializer<TypeConstraint> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new TypeConstraintSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }
}
