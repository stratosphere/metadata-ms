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
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import de.hpi.isg.mdms.model.targets.Column;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Constraint implementation for an n-ary unique column combination.
 * 
 * @author Sebastian Kruse
 */
public class UniqueColumnCombination extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class UniqueColumnCombinationSQLiteSerializer implements
            ConstraintSQLSerializer<UniqueColumnCombination> {

        private final static String tableName = "UCC";
        private final static String referenceTableName = "UCCPart";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertUniqueColumnCombinationWriter;

        DatabaseWriter<Integer> deleteUniqueColumnCombinationWriter;

        DatabaseWriter<int[]> insertUCCPartWriter;

        DatabaseWriter<Integer> deleteUCCPartWriter;

        DatabaseQuery<Void> queryUniqueColumnCombination;

        DatabaseQuery<Integer> queryUniqueColumnCombinationsForConstraintCollection;

        DatabaseQuery<Integer> queryUCCPart;

		private int currentConstraintIdMax = -1;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_UNIQECOLUMNCOMBINATION_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId, constraintCollectionId) VALUES (?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters[0]);
                                preparedStatement.setInt(2, parameters[1]);
                            }
                        },
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_UNIQECOLUMNCOMBINATION_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from " + tableName + " where constraintId=?;",
                        new PreparedStatementAdapter<Integer>() {
                            @Override
                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter);
                            }
                        },
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_UCCPART_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + referenceTableName
                                + " (constraintId, col) VALUES (?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters[0]);
                                preparedStatement.setInt(2, parameters[1]);
                            }
                        },
                        referenceTableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_UCCPART_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from " + referenceTableName
                                + " where constraintId=?;",
                        new PreparedStatementAdapter<Integer>() {
                            @Override
                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter);
                            }
                        },
                        referenceTableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> UNIQECOLUMNCOMBINATION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT " + tableName + ".constraintId as id, " + tableName + ".constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + ";",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> UNIQECOLUMNCOMBINATION_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT " + tableName + ".constraintId as id, " + tableName + ".constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + " where " + tableName + ".constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> UCCPART_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT col "
                                + "from " + referenceTableName + " "
                                + "where " + referenceTableName + ".constraintId = ?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        referenceTableName);

        public UniqueColumnCombinationSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertUniqueColumnCombinationWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_UNIQECOLUMNCOMBINATION_WRITER_FACTORY);

                this.deleteUniqueColumnCombinationWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_UNIQECOLUMNCOMBINATION_WRITER_FACTORY);

                this.insertUCCPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_UCCPART_WRITER_FACTORY);

                this.deleteUCCPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_UCCPART_WRITER_FACTORY);

                this.queryUniqueColumnCombination = sqlInterface.getDatabaseAccess().createQuery(
                        UNIQECOLUMNCOMBINATION_QUERY_FACTORY);

                this.queryUniqueColumnCombinationsForConstraintCollection = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                UNIQECOLUMNCOMBINATION_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);

                this.queryUCCPart = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                UCCPART_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint uniqueColumnCombination, ConstraintCollection constraintCollection) {

            Validate.isTrue(uniqueColumnCombination instanceof UniqueColumnCombination);
            
            ensureCurrentConstraintIdMaxInitialized();
            int constraintId = ++ currentConstraintIdMax;
            
            try {
                insertUniqueColumnCombinationWriter.write(new int[]{constraintId, constraintCollection.getId()});

                IntCollection targetIds = ((UniqueColumnCombination) uniqueColumnCombination).getTargetReference()
                        .getAllTargetIds();
                for (IntIterator i = targetIds.iterator(); i.hasNext();) {
                    insertUCCPartWriter.write(new int[] { constraintId, i.nextInt() });
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<UniqueColumnCombination> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<UniqueColumnCombination> uniqueColumnCombiantions = new HashSet<>();

            try {
                ResultSet rsUniqueColumnCombinations = retrieveConstraintCollection ?
                        queryUniqueColumnCombination.execute(null)
                        : queryUniqueColumnCombinationsForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsUniqueColumnCombinations.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsUniqueColumnCombinations
                                        .getInt("constraintCollectionId"));
                    }
                    uniqueColumnCombiantions
                            .add(UniqueColumnCombination.build(
                                    getUniqueColumnCombinationReferences(rsUniqueColumnCombinations.getInt("id")),
                                    constraintCollection));

                }
                rsUniqueColumnCombinations.close();

                return uniqueColumnCombiantions;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public Reference getUniqueColumnCombinationReferences(int id) {
            IntList cols = new IntArrayList();
            try {
                try (ResultSet rs = this.queryUCCPart.execute(id);) {
                    while (rs.next()) {
                        cols.add(rs.getInt("col"));
                    }
                }
                return new Reference(cols.toIntArray());
            } catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getTableNames() {
            return Arrays.asList(tableName, referenceTableName);
        }

        @Override
        public void initializeTables() {
            if (!sqlInterface.tableExists(tableName)) {
                String createINDTable = "CREATE TABLE [" + tableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [constraintCollectionId] integer NOT NULL,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "    FOREIGN KEY ([constraintCollectionId])\n" +
                        "    REFERENCES [ConstraintCollection] ([id])\n" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createINDTable);
            }
            if (!sqlInterface.tableExists(referenceTableName)) {
                String createINDpartTable = "CREATE TABLE [" + referenceTableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [col] integer NOT NULL,\n" +
                        "    FOREIGN KEY ([col])\n" +
                        "    REFERENCES [Columnn] ([id]),\n" +
                        "    FOREIGN KEY ([constraintId])\n" +
                        "    REFERENCES [" + tableName + "] ([constraintId])" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createINDpartTable);
            }
            // check again and set allTablesExistChecked to true
            if (!(sqlInterface.tableExists(tableName) && sqlInterface.tableExists(referenceTableName))) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            try {
                ResultSet rsUCCs = queryUniqueColumnCombinationsForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsUCCs.next()) {
                    this.deleteUniqueColumnCombinationWriter.write(rsUCCs.getInt("id"));
                    this.deleteUCCPartWriter.write(rsUCCs.getInt("id"));
                }
                rsUCCs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
                
        private void ensureCurrentConstraintIdMaxInitialized() {
            if (this.currentConstraintIdMax != -1) {
                return;
            }
    
            try {
                this.currentConstraintIdMax = 0;
                	try (ResultSet res = this.sqlInterface.getDatabaseAccess().query("SELECT MAX(constraintId) from " + getTableNames().get(0) + ";", getTableNames().get(0))) {
                        while (res.next()) {
                        	if (this.currentConstraintIdMax < res.getInt("max(constraintId)")) {
                                this.currentConstraintIdMax = res.getInt("max(constraintId)");                    		
                        	}
                        }
                    }	
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -3272378011671591628L;

        @SuppressWarnings("unused")
        private static int[] toIntArray(Column[] columns) {
            int[] intArray = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                intArray[i] = columns[i].getId();
            }
            return intArray;
        }

        int[] uniqueColumns;

        public Reference(final int[] uniqueColumns) {
            this.uniqueColumns = uniqueColumns;
            Arrays.sort(this.uniqueColumns);
        }

        @Override
        public IntCollection getAllTargetIds() {
            return new IntArrayList(this.uniqueColumns);
        }

        @Override
        public String toString() {
            return "Reference [uniqueColumns=" + Arrays.toString(uniqueColumns) + "]";
        }
    }

    private static final long serialVersionUID = -932394088609862495L;
    private UniqueColumnCombination.Reference target;

    @Deprecated
    public static UniqueColumnCombination build(final UniqueColumnCombination.Reference target,
            ConstraintCollection constraintCollection) {
        UniqueColumnCombination uniqueColumnCombination = new UniqueColumnCombination(target);
        return uniqueColumnCombination;
    }

    public static UniqueColumnCombination buildAndAddToCollection(final UniqueColumnCombination.Reference target,
            ConstraintCollection<UniqueColumnCombination> constraintCollection) {
        UniqueColumnCombination uniqueColumnCombination = new UniqueColumnCombination(target);
        constraintCollection.add(uniqueColumnCombination);
        return uniqueColumnCombination;
    }


    public UniqueColumnCombination(final UniqueColumnCombination.Reference target) {
        this.target = target;
    }

    @Override
    public UniqueColumnCombination.Reference getTargetReference() {
        return target;
    }

    @Override
    public String toString() {
        return "UniqueColumnCombination [target=" + target + "]";
    }

    public int getArity() {
        return this.getTargetReference().uniqueColumns.length;
    }

    @Override
    public ConstraintSQLSerializer<UniqueColumnCombination> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new UniqueColumnCombinationSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}