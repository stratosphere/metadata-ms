/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraint;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.model.constraints.AbstractConstraint;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Constraint implementation for the number of tuples in a target.
 * 
 * @author Sebastian Kruse
 */
public class NumberedDummyConstraint extends AbstractConstraint implements RDBMSConstraint {

    public static class DummySQLiteSerializer implements ConstraintSQLSerializer<NumberedDummyConstraint> {

        private final static String tableName = "dummy";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertDummyWriter;

        DatabaseQuery<Void> queryConstraints;

        DatabaseQuery<Integer> queryConstraintsForConstraintCollection;

        DatabaseWriter<Integer> removeWriter;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_DUMMY_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintCollectionId, dummy, columnId) VALUES (?, ?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters[0]);
                                preparedStatement.setInt(2, parameters[1]);
                                preparedStatement.setInt(3, parameters[2]);
                            }
                        },
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> REMOVE_DUMMY_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE FROM " + tableName + " WHERE constraintCollectionId = ?;",
                        new PreparedStatementAdapter<Integer>() {
                            @Override
                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter);
                            }
                        },
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> DUMMY_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT dummy.constraintId as id, dummy.columnId as columnId, dummy.dummy as dummy,"
                                + " dummy.constraintCollectionId as constraintCollectionId"
                                + " from dummy;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> DUMMY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT dummy.constraintId as id, dummy.columnId as columnId, dummy.dummy as dummy,"
                                + " dummy.constraintCollectionId as constraintCollectionId"
                                + " from dummy where dummy.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public DummySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertDummyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_DUMMY_WRITER_FACTORY);

                this.queryConstraints = sqlInterface.getDatabaseAccess().createQuery(
                        DUMMY_QUERY_FACTORY);

                this.queryConstraintsForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
                        DUMMY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);

                this.removeWriter = sqlInterface.getDatabaseAccess().createBatchWriter(REMOVE_DUMMY_WRITER_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint dummy) {
            Validate.isTrue(dummy instanceof NumberedDummyConstraint);
            try {
                insertDummyWriter.write(new int[]{
                        dummy.getConstraintCollection().getId(), ((NumberedDummyConstraint) dummy).getValue(), dummy
                        .getTargetReference()
                        .getAllTargetIds().iterator()
                        .nextInt()
                });
                
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<NumberedDummyConstraint> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<NumberedDummyConstraint> dummys = new HashSet<>();

            try {
                ResultSet rsdummys = retrieveConstraintCollection ?
                        queryConstraints.execute(null) : queryConstraintsForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsdummys.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsdummys
                                        .getInt("constraintCollectionId"));
                    }
                    dummys
                            .add(NumberedDummyConstraint.build(
                                    new NumberedDummyConstraint.Reference(this.sqlInterface.getColumnById(rsdummys
                                            .getInt("columnId"))), constraintCollection,
                                    rsdummys.getInt("dummy")));
                }
                rsdummys.close();

                return dummys;
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
                        "    [dummy] integer,\n" +
                        "    PRIMARY KEY ([constraintId])\n" +
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
                this.removeWriter.write(constraintCollection.getId());
                this.removeWriter.flush();
            } catch (SQLException e) {
                throw new RuntimeException("Could not delete constraints.", e);
            }
        }
    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Target target;

        public Reference(final Target target) {
            this.target = target;
        }

        @Override
        public IntCollection getAllTargetIds() {
            return IntLists.singleton(this.target.getId());
        }

        @Override
        public String toString() {
            return "Reference [target=" + target + "]";
        }

    }

    private static final long serialVersionUID = -932394088609862495L;

    private int value;

    private Reference target;

    /**
     * @see de.hpi.isg.mdms.model.constraints.AbstractConstraint
     */
    private NumberedDummyConstraint(final Reference target,
                                    final ConstraintCollection constraintCollection, int value) {

        super(constraintCollection);
        this.target = target;
        this.value = value;
    }

    public static NumberedDummyConstraint build(final Reference target, ConstraintCollection constraintCollection,
            int numTuples) {
        NumberedDummyConstraint dummy = new NumberedDummyConstraint(target, constraintCollection, numTuples);
        return dummy;
    }

    public static NumberedDummyConstraint buildAndAddToCollection(final Reference target,
            ConstraintCollection constraintCollection,
            int numTuples) {
        NumberedDummyConstraint dummy = new NumberedDummyConstraint(target, constraintCollection, numTuples);
        constraintCollection.add(dummy);
        return dummy;
    }

    public static NumberedDummyConstraint buildAndAddToCollection(final Target target,
            ConstraintCollection constraintCollection,
            int numTuples) {
        return buildAndAddToCollection(new NumberedDummyConstraint.Reference(target), constraintCollection, numTuples);
    }

    @Override
    public NumberedDummyConstraint.Reference getTargetReference() {
        return this.target;
    }

    /**
     * @return the numDistinctValues
     */
    public int getValue() {
        return value;
    }

    /**
     * @param numDistinctValues
     *        the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.value = numDistinctValues;
    }

    @Override
    public String toString() {
        return "dummy[" + getTargetReference() + ", value=" + value + "]";
    }

    @Override
    public ConstraintSQLSerializer<NumberedDummyConstraint> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new DummySQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}