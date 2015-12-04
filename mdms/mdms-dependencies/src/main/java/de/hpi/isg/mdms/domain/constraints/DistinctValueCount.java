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
package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Constraint implementation distinct value counts of a single column.
 * 
 * @author Sebastian Kruse
 */
public class DistinctValueCount extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class DistinctValueCountSQLiteSerializer implements ConstraintSQLSerializer<DistinctValueCount> {

        private final static String tableName = "DistinctValueCount";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertDistinctValueCountWriter;

        DatabaseWriter<Integer> deleteDistinctValueCountWriter;

        DatabaseQuery<Void> queryDistinctValueCount;

        DatabaseQuery<Integer> queryDistinctValueCountForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_DISTINCTVALUECOUNT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName
                                + " (constraintCollectionId, distinctValueCount, columnId) VALUES (?, ?, ?);",
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

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_DISTINCTVALUECOUNT_WRITER_FACTORY =
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

        private static final StrategyBasedPreparedQuery.Factory<Void> DISTINCTVALUECOUNT_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT DistinctValueCount.constraintId as id, DistinctValueCount.columnId as columnId,"
                				+ " DistinctValueCount.constraintCollectionId as constraintCollectionId,"
                                + " DistinctValueCount.distinctValueCount as distinctValueCount,"
                                + " DistinctValueCount.constraintCollectionId as constraintCollectionId"
                                + " from DistinctValueCount;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> DISTINCTVALUECOUNT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT DistinctValueCount.constraintId as id, DistinctValueCount.columnId as columnId,"
                                + " DistinctValueCount.distinctValueCount as distinctValueCount,"
                				+ " DistinctValueCount.constraintCollectionId as constraintCollectionId"
                                + " from DistinctValueCount where DistinctValueCount.constraintCollectionId =?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public DistinctValueCountSQLiteSerializer(SQLInterface sqliteInterface) {
            this.sqlInterface = sqliteInterface;

            try {
                this.insertDistinctValueCountWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_DISTINCTVALUECOUNT_WRITER_FACTORY);

                this.deleteDistinctValueCountWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_DISTINCTVALUECOUNT_WRITER_FACTORY);

                this.queryDistinctValueCount = sqlInterface.getDatabaseAccess().createQuery(
                        DISTINCTVALUECOUNT_QUERY_FACTORY);

                this.queryDistinctValueCountForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
                        DISTINCTVALUECOUNT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint distinctValueCount, ConstraintCollection constraintCollection) {
            Validate.isTrue(distinctValueCount instanceof DistinctValueCount);
            try {
                this.insertDistinctValueCountWriter.write(new int[] { constraintCollection.getId(),
                        ((DistinctValueCount) distinctValueCount).getNumDistinctValues(), distinctValueCount
                                .getTargetReference().getAllTargetIds().iterator().nextInt() });

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<DistinctValueCount> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<DistinctValueCount> distinctValueCounts = new HashSet<>();

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
                            .add(new DistinctValueCount(
                                new SingleTargetReference(rsDistinctValueCounts.getInt("columnId")),
                                rsDistinctValueCounts.getInt("distinctValueCount")));
                }
                rsDistinctValueCounts.close();
                return distinctValueCounts;
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
                        "	 [constraintCollectionId] integer NOT NULL,\n" +
                        "    [columnId] integer NOT NULL,\n" +
                        "    [distinctValueCount] integer,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "    FOREIGN KEY ([columnId])\n" +
                        "    REFERENCES [Columnn] ([id]),\n" +
                        "	 FOREIGN KEY ([constraintCollectionId])" +
                        "    REFERENCES [ConstraintCollection] ([id])" +
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
                ResultSet rsDistinctValueCounts = queryDistinctValueCountForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsDistinctValueCounts.next()) {
                    this.deleteDistinctValueCountWriter.write(rsDistinctValueCounts.getInt("id"));
                }
                rsDistinctValueCounts.close();

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final long serialVersionUID = -932394088609862495L;

    private int numDistinctValues;

    private SingleTargetReference target;

    public DistinctValueCount(final SingleTargetReference target, int numDistinctValues) {

        this.target = target;
        this.numDistinctValues = numDistinctValues;
    }

    @Deprecated
    public static DistinctValueCount build(final SingleTargetReference target,
           int numDistinctValues) {
        DistinctValueCount distinctValueCount = new DistinctValueCount(target, numDistinctValues);
        return distinctValueCount;
    }

    public static DistinctValueCount buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            int numDistinctValues) {
        DistinctValueCount distinctValueCount = new DistinctValueCount(target, numDistinctValues);
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
    public ConstraintSQLSerializer<DistinctValueCount> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new DistinctValueCountSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }
}