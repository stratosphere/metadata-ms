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
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter.Factory;
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
 * Constraint implementation for a functional dependency.
 * 
 */
public class FunctionalDependency extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class FunctionalDependencySQLiteSerializer implements
            ConstraintSQLSerializer<FunctionalDependency> {

        private final static String tableName = "FD";
        private final static String lhsTableName = "FD_LHS";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertFunctionalDependencyWriter;

        DatabaseWriter<Integer> deleteFunctionalDependencyWriter;

        DatabaseWriter<int[]> insertFDLhstWriter;

        DatabaseWriter<Integer> deleteFDLhsWriter;

        DatabaseQuery<Void> queryFunctionalDependency;

        DatabaseQuery<Integer> queryFunctionalDependencyForConstraintCollection;

        DatabaseQuery<Integer> queryFDLhs;

        private static final Factory<int[]> INSERT_FUNCTIONALDEPENDENCY_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId, rhs_col) VALUES (?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters[0]);
                                preparedStatement.setInt(2, parameters[1]);
                            }
                        },
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_FUNCTIONALDEPENDENCY_WRITER_FACTORY =
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

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_FDLHS_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + lhsTableName
                                + " (constraintId, lhs_col) VALUES (?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters[0]);
                                preparedStatement.setInt(2, parameters[1]);
                            }
                        },
                        lhsTableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_FDLHS_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from " + lhsTableName
                                + " where constraintId=?;",
                        new PreparedStatementAdapter<Integer>() {
                            @Override
                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter);
                            }
                        },
                        lhsTableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> FUNCTIONALDEPENDENCY_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + ", constraintt where " + tableName
                                + ".constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);
                
        private static final StrategyBasedPreparedQuery.Factory<Integer> FUNCTIONALDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + ", constraintt where " + tableName
                                + ".constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);
                
        private static final StrategyBasedPreparedQuery.Factory<Integer> FDLHS_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT lhs_col, rhs_col "
                                + "from " + lhsTableName + ", " + tableName + " "
                                + "where " + lhsTableName + ".constraintId = ? "
                                		+ "and " + lhsTableName + ".constraintId = " + tableName + ".constraintId;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        lhsTableName);

        public FunctionalDependencySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertFunctionalDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_FUNCTIONALDEPENDENCY_WRITER_FACTORY);

                this.deleteFunctionalDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_FUNCTIONALDEPENDENCY_WRITER_FACTORY);

                this.insertFDLhstWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_FDLHS_WRITER_FACTORY);

                this.deleteFDLhsWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_FDLHS_WRITER_FACTORY);

                this.queryFunctionalDependency = sqlInterface.getDatabaseAccess().createQuery(
                        FUNCTIONALDEPENDENCY_QUERY_FACTORY);

                this.queryFunctionalDependencyForConstraintCollection = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                FUNCTIONALDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);

                this.queryFDLhs = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                FDLHS_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Integer constraintId, Constraint functionalDependency) {

            Validate.isTrue(functionalDependency instanceof FunctionalDependency);
            try {
            	Reference fd = ((FunctionalDependency) functionalDependency).getTargetReference();
                insertFunctionalDependencyWriter.write(new int[] {constraintId, fd.getRHSTarget()});
                IntCollection targetIds = fd.getLHSTargetIds();
                for (IntIterator i = targetIds.iterator(); i.hasNext();) {
                    insertFDLhstWriter.write(new int[] { constraintId, i.nextInt() });
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<FunctionalDependency> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<FunctionalDependency> functionDependencies = new HashSet<>();

            try {
                ResultSet rsFunctionalDependencies = retrieveConstraintCollection ?
                        queryFunctionalDependency.execute(null)
                        : queryFunctionalDependencyForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsFunctionalDependencies.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsFunctionalDependencies
                                        .getInt("constraintCollectionId"));
                    }
                    functionDependencies
                            .add(new FunctionalDependency(
                                    getFunctionalDependencyReferences(rsFunctionalDependencies.getInt("id"))));

                }
                rsFunctionalDependencies.close();

                return functionDependencies;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public Reference getFunctionalDependencyReferences(int id) {
            IntList lhs_cols = new IntArrayList();
            Integer rhs_col = null;
            try {
                try (ResultSet rs = this.queryFDLhs.execute(id);) {
                    while (rs.next()) {
                    	if (rhs_col == null){
                    		rhs_col = rs.getInt("rhs_col");
                    	}
                        lhs_cols.add(rs.getInt("lhs_col"));
                        
                    }
                }
                return new Reference(rhs_col, lhs_cols.toIntArray());
            } catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getTableNames() {
            return Arrays.asList(tableName, lhsTableName);
        }

        @Override
        public void initializeTables() {
            if (!sqlInterface.tableExists(tableName)) {
                String createFDTable = "CREATE TABLE [" + tableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [rhs_col] integer,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "    FOREIGN KEY ([constraintId])\n" +
                        "    REFERENCES [Constraintt] ([id]),\n" +
                        "    FOREIGN KEY ([rhs_col])\n" +
                        "    REFERENCES [Columnn] ([id])" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createFDTable);
            }
            if (!sqlInterface.tableExists(lhsTableName)) {
                String createFDlhsTable = "CREATE TABLE [" + lhsTableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [lhs_col] integer NOT NULL,\n" +
                        "    FOREIGN KEY ([lhs_col])\n" +
                        "    REFERENCES [Columnn] ([id]),\n" +
                        "    FOREIGN KEY ([constraintId])\n" +
                        "    REFERENCES [" + tableName + "] ([constraintId])" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createFDlhsTable);
            }
            // check again and set allTablesExistChecked to true
            if (!(sqlInterface.tableExists(tableName) && sqlInterface.tableExists(lhsTableName))) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            try {
                ResultSet rsFDs = queryFunctionalDependencyForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsFDs.next()) {
                    this.deleteFunctionalDependencyWriter.write(rsFDs.getInt("id"));
                    this.deleteFDLhsWriter.write(rsFDs.getInt("id"));
                }
                rsFDs.close();
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

        int[] lhs_columns;
        int rhs_column;

        public Reference(final int rhs_column, final int[] lhs_columns) {
            this.lhs_columns = lhs_columns;
            Arrays.sort(this.lhs_columns);
            this.rhs_column = rhs_column;
        }

        public IntCollection getLHSTargetIds(){
        	return new IntArrayList(this.lhs_columns);
        }
        
        public int getRHSTarget(){
        	return this.rhs_column;
        }
        
        @Override
        public IntCollection getAllTargetIds() {
        	 IntArrayList targetList = new IntArrayList(this.lhs_columns);
        	 targetList.add(rhs_column);
            return targetList;
        }

        @Override
        public String toString() {
            return "Reference [functionalDependency=" + Arrays.toString(lhs_columns) + "-->" + rhs_column + "]";
        }
    }

    private static final long serialVersionUID = -932394088609862495L;
    private FunctionalDependency.Reference target;

  /**
   * @deprecated use {@link #FunctionalDependency} instead
   */
    public static FunctionalDependency build(final FunctionalDependency.Reference target) {
        FunctionalDependency functionalDependency = new FunctionalDependency(target);
        return functionalDependency;
    }

    public static FunctionalDependency buildAndAddToCollection(final FunctionalDependency.Reference target,
            ConstraintCollection constraintCollection) {
        FunctionalDependency functionalDependency = new FunctionalDependency(target);
        constraintCollection.add(functionalDependency);
        return functionalDependency;
    }


    public FunctionalDependency(final FunctionalDependency.Reference target) {
        this.target = target;
    }

    @Override
    public FunctionalDependency.Reference getTargetReference() {
        return target;
    }

    @Override
    public String toString() {
        return "FunctionalDependency [target=" + target + "]";
    }

    public int getArity() {
        return this.getTargetReference().lhs_columns.length;
    }

    @Override
    public ConstraintSQLSerializer<FunctionalDependency> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new FunctionalDependencySQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}