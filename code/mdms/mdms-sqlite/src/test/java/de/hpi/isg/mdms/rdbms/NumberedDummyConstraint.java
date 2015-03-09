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
import de.hpi.isg.mdms.domain.*;
import de.hpi.isg.mdms.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.domain.constraints.impl.AbstractConstraint;
import de.hpi.isg.mdms.domain.constraints.impl.ConstraintSQLSerializer;
import de.hpi.isg.mdms.domain.factories.SQLInterface;
import de.hpi.isg.mdms.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.mdms.factories.SQLiteInterface;
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
 * Constraint implementation for the number of tuples in a table.
 * 
 * @author Sebastian Kruse
 */
public class NumberedDummyConstraint extends AbstractConstraint implements RDBMSConstraint {

    public static class DummySQLiteSerializer implements ConstraintSQLSerializer<NumberedDummyConstraint> {

        private final static String tableName = "dummy";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertdummyWriter;

        DatabaseQuery<Void> querydummys;

        DatabaseQuery<Integer> querydummysForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_dummy_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId, dummy, tableId) VALUES (?, ?, ?);",
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

        private static final StrategyBasedPreparedQuery.Factory<Void> dummy_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, dummy.tableId as tableId, dummy.dummy as dummy,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from dummy, constraintt where dummy.constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> dummy_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, dummy.tableId as tableId, dummy.dummy as dummy,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from dummy, constraintt where dummy.constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public DummySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertdummyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_dummy_WRITER_FACTORY);

                this.querydummys = sqlInterface.getDatabaseAccess().createQuery(
                        dummy_QUERY_FACTORY);

                this.querydummysForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
                        dummy_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Integer constraintId, Constraint dummy) {
            Validate.isTrue(dummy instanceof NumberedDummyConstraint);
            try {
                insertdummyWriter.write(new int[] {
                        constraintId, ((NumberedDummyConstraint) dummy).getNumTuples(), dummy
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
                        querydummys.execute(null) : querydummysForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsdummys.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsdummys
                                        .getInt("constraintCollectionId"));
                    }
                    dummys
                            .add(NumberedDummyConstraint.build(
                                    new NumberedDummyConstraint.Reference(this.sqlInterface.getTableById(rsdummys
                                            .getInt("tableId"))), constraintCollection,
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
                        "    [tableId] integer NOT NULL,\n" +
                        "    [dummy] integer,\n" +
                        "    FOREIGN KEY ([constraintId])\n" +
                        "    REFERENCES [Constraintt] ([id]),\n" +
                        "    FOREIGN KEY ([tableId])\n" +
                        "    REFERENCES [Tablee] ([id])\n" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createTable);
            }
            if (!sqlInterface.tableExists(tableName)) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Not supported yet.");
            //
        }
    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Target table;

        public Reference(final Target target) {
            this.table = target;
        }

        @Override
        public IntCollection getAllTargetIds() {
            return IntLists.singleton(this.table.getId());
        }

        @Override
        public String toString() {
            return "Reference [table=" + table + "]";
        }

    }

    private static final long serialVersionUID = -932394088609862495L;

    private int numTuples;

    private Reference target;

    /**
     * @see de.hpi.isg.mdms.domain.constraints.impl.AbstractConstraint
     */
    private NumberedDummyConstraint(final Reference target,
                                    final ConstraintCollection constraintCollection, int numTuples) {

        super(constraintCollection);
        this.target = target;
        this.numTuples = numTuples;
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
    public int getNumTuples() {
        return numTuples;
    }

    /**
     * @param numDistinctValues
     *        the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numTuples = numDistinctValues;
    }

    @Override
    public String toString() {
        return "dummy[" + getTargetReference() + ", numTuples=" + numTuples + "]";
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