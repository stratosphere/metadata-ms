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
package de.hpi.isg.mdms.domain.constraints.impl;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.Constraint;
import de.hpi.isg.mdms.domain.ConstraintCollection;
import de.hpi.isg.mdms.domain.factories.SQLInterface;
import de.hpi.isg.mdms.domain.factories.SQLiteInterface;
import de.hpi.isg.mdms.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.mdms.domain.impl.SingleTargetReference;
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
public class TupleCount extends AbstractConstraint {

    public static class TupleCountSQLiteSerializer implements ConstraintSQLSerializer<TupleCount> {

        private final static String tableName = "TupleCount";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertTupleCountWriter;

        DatabaseWriter<Integer> deleteTupleCountWriter;

        DatabaseQuery<Void> queryTupleCounts;

        DatabaseQuery<Integer> queryTupleCountsForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_TUPLECOUNT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId, tupleCount, tableId) VALUES (?, ?, ?);",
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

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_TUPLECOUNT_WRITER_FACTORY =
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

        private static final StrategyBasedPreparedQuery.Factory<Void> TUPLECOUNT_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, TupleCount.tableId as tableId, TupleCount.tupleCount as tupleCount,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from TupleCount, constraintt where TupleCount.constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> TUPLECOUNT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, TupleCount.tableId as tableId, TupleCount.tupleCount as tupleCount,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from TupleCount, constraintt where TupleCount.constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public TupleCountSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertTupleCountWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_TUPLECOUNT_WRITER_FACTORY);

                this.deleteTupleCountWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_TUPLECOUNT_WRITER_FACTORY);

                this.queryTupleCounts = sqlInterface.getDatabaseAccess().createQuery(
                        TUPLECOUNT_QUERY_FACTORY);

                this.queryTupleCountsForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
                        TUPLECOUNT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Integer constraintId, Constraint tupleCount) {
            Validate.isTrue(tupleCount instanceof TupleCount);
            try {
                insertTupleCountWriter.write(new int[] {
                        constraintId, ((TupleCount) tupleCount).getNumTuples(), tupleCount
                                .getTargetReference()
                                .getAllTargetIds().iterator().nextInt()
                });

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<TupleCount> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<TupleCount> tupleCounts = new HashSet<>();

            try {
                ResultSet rsTupleCounts = retrieveConstraintCollection ?
                        queryTupleCounts.execute(null) : queryTupleCountsForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsTupleCounts.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsTupleCounts
                                        .getInt("constraintCollectionId"));
                    }
                    tupleCounts
                            .add(TupleCount.build(
                                    new SingleTargetReference(this.sqlInterface.getTableById(rsTupleCounts
                                            .getInt("tableId")).getId()), constraintCollection,
                                    rsTupleCounts.getInt("tupleCount")));
                }
                rsTupleCounts.close();

                return tupleCounts;
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
                        "    [tupleCount] integer,\n" +
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
            try {
                ResultSet rsTupleCounts = queryTupleCountsForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsTupleCounts.next()) {
                    this.deleteTupleCountWriter.write(rsTupleCounts.getInt("id"));
                }
                rsTupleCounts.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final long serialVersionUID = -932394088609862495L;

    private int numTuples;

    private SingleTargetReference target;

    /**
     * @see AbstractConstraint
     */
    private TupleCount(final SingleTargetReference target,
            final ConstraintCollection constraintCollection, int numTuples) {

        super(constraintCollection);
        this.target = target;
        this.numTuples = numTuples;
    }

    public static TupleCount build(final SingleTargetReference target, ConstraintCollection constraintCollection,
            int numTuples) {
        TupleCount tupleCount = new TupleCount(target, constraintCollection, numTuples);
        return tupleCount;
    }

    public static TupleCount buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            int numTuples) {
        TupleCount tupleCount = new TupleCount(target, constraintCollection, numTuples);
        constraintCollection.add(tupleCount);
        return tupleCount;
    }

    @Override
    public SingleTargetReference getTargetReference() {
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
        return "TupleCount[" + getTargetReference() + ", numTuples=" + numTuples + "]";
    }

    @Override
    public ConstraintSQLSerializer<TupleCount> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new TupleCountSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}