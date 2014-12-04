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
package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;

/**
 * Constraint implementation distinct value counts of a single column.
 * 
 * @author Sebastian Kruse
 */
public class DistinctValueCount extends AbstractConstraint {

    public static class DistinctValueCountSQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "DistinctValueCount";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertDistinctValueCountWriter;

        DatabaseQuery<Void> queryDistinctValueCount;

        DatabaseQuery<Integer> queryDistinctValueCountForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_DISTINCTVALUECOUNT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName
                                + " (constraintId, distinctValueCount, columnId) VALUES (?, ?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, (Integer) parameters[0]);
                                preparedStatement.setString(2, String.valueOf(parameters[1]));
                                preparedStatement.setInt(3, (Integer) parameters[2]);
                            }
                        },
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> DISTINCTVALUECOUNT_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, DistinctValueCount.columnId as columnId,"
                                + " DistinctValueCount.distinctValueCount as distinctValueCount,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from DistinctValueCount, constraintt where DistinctValueCount.constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> DISTINCTVALUECOUNT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, DistinctValueCount.columnId as columnId,"
                                + " DistinctValueCount.distinctValueCount as distinctValueCount,"
                                + " constraintt.constraintCollectionId as constraintCollectionId"
                                + " from DistinctValueCount, constraintt where DistinctValueCount.constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public DistinctValueCountSQLiteSerializer(SQLInterface sqliteInterface) {
            this.sqlInterface = sqliteInterface;

            if (!allTablesExistChecked) {
                if (!sqliteInterface.tableExists(tableName)) {
                    String createTable = "CREATE TABLE [" + tableName + "]\n" +
                            "(\n" +
                            "    [constraintId] integer NOT NULL,\n" +
                            "    [columnId] integer NOT NULL,\n" +
                            "    [distinctValueCount] integer,\n" +
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
                this.insertDistinctValueCountWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_DISTINCTVALUECOUNT_WRITER_FACTORY);

                this.queryDistinctValueCount = sqliteInterface.getDatabaseAccess().createQuery(
                        DISTINCTVALUECOUNT_QUERY_FACTORY);

                this.queryDistinctValueCountForConstraintCollection = sqliteInterface.getDatabaseAccess().createQuery(
                        DISTINCTVALUECOUNT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Integer constraintId, Constraint distinctValueCount) {
            Validate.isTrue(distinctValueCount instanceof DistinctValueCount);
            try {
                this.insertDistinctValueCountWriter.write(new int[] { constraintId,
                        ((DistinctValueCount) distinctValueCount).getNumDistinctValues(), distinctValueCount
                                .getTargetReference().getAllTargets().iterator().next().getId() });

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<Constraint> deserializeConstraintsForConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<Constraint> distinctValueCounts = new HashSet<>();

            try {

                ResultSet rsDistinctValueCounts = retrieveConstraintCollection ?
                        queryDistinctValueCount.execute(null) : queryDistinctValueCountForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsDistinctValueCounts.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsDistinctValueCounts
                                        .getInt("constraintCollectionId"));
                    }
                    distinctValueCounts
                            .add(DistinctValueCount.build(
                                    new SingleTargetReference(this.sqlInterface.getColumnById(rsDistinctValueCounts
                                            .getInt("columnId"))), constraintCollection,
                                    rsDistinctValueCounts.getInt("distinctValueCount")));
                }
                rsDistinctValueCounts.close();
                return distinctValueCounts;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final long serialVersionUID = -932394088609862495L;

    private int numDistinctValues;

    private SingleTargetReference target;

    /**
     * @see AbstractConstraint
     */
    public DistinctValueCount(final SingleTargetReference target,
            final ConstraintCollection constraintCollection, int numDistinctValues) {

        super(constraintCollection);
        this.target = target;
        this.numDistinctValues = numDistinctValues;
    }

    public static DistinctValueCount build(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            int numDistinctValues) {
        DistinctValueCount distinctValueCount = new DistinctValueCount(target, constraintCollection,
                numDistinctValues);
        return distinctValueCount;
    }

    public static DistinctValueCount buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            int numDistinctValues) {
        DistinctValueCount distinctValueCount = new DistinctValueCount(target, constraintCollection,
                numDistinctValues);
        constraintCollection.add(distinctValueCount);
        return distinctValueCount;
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.target;
    }

    /**
     * @return the numDistinctValues
     */
    public int getNumDistinctValues() {
        return numDistinctValues;
    }

    /**
     * @param numDistinctValues
     *        the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numDistinctValues = numDistinctValues;
    }

    @Override
    public String toString() {
        return "DistinctValueCount[" + getTargetReference() + ", numDistinctValues=" + numDistinctValues + "]";
    }

    @Override
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new DistinctValueCountSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}