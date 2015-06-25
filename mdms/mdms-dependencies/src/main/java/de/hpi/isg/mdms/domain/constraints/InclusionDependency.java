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
import de.hpi.isg.mdms.model.constraints.AbstractConstraint;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntList;

import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Constraint implementation for an n-ary inclusion dependency.
 * 
 * @author Sebastian Kruse
 */
public class InclusionDependency extends AbstractConstraint implements RDBMSConstraint {

    public static class InclusionDependencySQLiteSerializer implements ConstraintSQLSerializer<InclusionDependency> {

        private final static String tableName = "IND";
        private final static String referenceTableName = "INDPart";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertInclusionDependencyWriter;

        DatabaseWriter<Integer> deleteInclusionDependencyWriter;

        DatabaseWriter<int[]> insertINDPartWriter;

        DatabaseWriter<Integer> deleteINDPartWriter;

        DatabaseQuery<Void> queryInclusionDependencies;

        DatabaseQuery<Integer> queryInclusionDependenciesForConstraintCollection;

        DatabaseQuery<Integer> queryINDPart;
		private int currentConstraintIdMax;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_INCLUSIONDEPENDENCY_WRITER_FACTORY =
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

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_INCLUSIONDEPENDENCY_WRITER_FACTORY =
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

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_INDPART_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + referenceTableName
                                + " (constraintId, lhs, rhs) VALUES (?, ?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters[0]);
                                preparedStatement.setInt(2, parameters[1]);
                                preparedStatement.setInt(3, parameters[2]);
                            }
                        },
                        referenceTableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_INDPART_WRITER_FACTORY =
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

        private static final StrategyBasedPreparedQuery.Factory<Void> INCLUSIONDEPENDENCY_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT " + tableName + ".constraintId as id, " + tableName + ".constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + " ;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> INCLUSIONDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT " + tableName + ".constraintId as id, " + tableName + ".constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + " where " + tableName
                                + ".constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> INDPART_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT lhs, rhs "
                                + "from " + referenceTableName + " "
                                + "where " + referenceTableName + ".constraintId = ?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        referenceTableName);

        public InclusionDependencySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.deleteInclusionDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_INCLUSIONDEPENDENCY_WRITER_FACTORY);

                this.insertInclusionDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_INCLUSIONDEPENDENCY_WRITER_FACTORY);

                this.deleteINDPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_INDPART_WRITER_FACTORY);

                this.insertINDPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_INDPART_WRITER_FACTORY);

                this.queryInclusionDependencies = sqlInterface.getDatabaseAccess().createQuery(
                        INCLUSIONDEPENDENCY_QUERY_FACTORY);

                this.queryInclusionDependenciesForConstraintCollection = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                INCLUSIONDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);

                this.queryINDPart = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                INDPART_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint inclusionDependency) {
        	ensureCurrentConstraintIdMaxInitialized();
        	
        	// for auto-increment id
        	Integer constraintId = ++currentConstraintIdMax;

            Validate.isTrue(inclusionDependency instanceof InclusionDependency);
            try {
                insertInclusionDependencyWriter.write(new int[] {constraintId, inclusionDependency.getConstraintCollection().getId()});

                for (int i = 0; i < ((InclusionDependency) inclusionDependency).getArity(); i++) {
                    insertINDPartWriter.write(new int[] { constraintId,
                            ((InclusionDependency) inclusionDependency).getTargetReference()
                                    .getDependentColumns()[i],
                            ((InclusionDependency) inclusionDependency).getTargetReference()
                                    .getReferencedColumns()[i] });
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<InclusionDependency> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<InclusionDependency> inclusionDependencies = new HashSet<>();

            try {
                ResultSet rsInclusionDependencies = retrieveConstraintCollection ?
                        queryInclusionDependencies.execute(null) : queryInclusionDependenciesForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsInclusionDependencies.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsInclusionDependencies
                                        .getInt("constraintCollectionId"));
                    }
                    inclusionDependencies
                            .add(InclusionDependency.build(
                                    getInclusionDependencyReferences(rsInclusionDependencies.getInt("id")),
                                    constraintCollection));

                }
                rsInclusionDependencies.close();

                return inclusionDependencies;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public Reference getInclusionDependencyReferences(int id) {
            List<Column> lhs = new ArrayList<>();
            List<Column> rhs = new ArrayList<>();
            try {
                try (ResultSet rs = this.queryINDPart.execute(id);) {
                    while (rs.next()) {
                        lhs.add(this.sqlInterface.getColumnById(rs.getInt("lhs")));
                        rhs.add(this.sqlInterface.getColumnById(rs.getInt("rhs")));
                    }
                }
                return new Reference(lhs.toArray(new Column[lhs.size()]), rhs.toArray(new Column[rhs.size()]));
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
                        "	 [constraintCollectionId] integer NOT NULL,\n" +                        
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "	 FOREIGN KEY ([constraintCollectionId])" +
                        "    REFERENCES [ConstraintCollection] ([id])"+
                        ");";
                this.sqlInterface.executeCreateTableStatement(createINDTable);
            }
            if (!sqlInterface.tableExists(referenceTableName)) {
                String createINDpartTable = "CREATE TABLE [" + referenceTableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [lhs] integer NOT NULL,\n" +
                        "    [rhs] integer NOT NULL,\n" +
                        "    FOREIGN KEY ([lhs])\n" +
                        "    REFERENCES [Columnn] ([id]),\n" +
                        "    FOREIGN KEY ([constraintId])\n" +
                        "    REFERENCES [" + tableName + "] ([constraintId]),\n" +
                        "    FOREIGN KEY ([rhs])\n" +
                        "    REFERENCES [Columnn] ([id])\n" +
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
                ResultSet rsINDs = queryInclusionDependenciesForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsINDs.next()) {
                    this.deleteInclusionDependencyWriter.write(rsINDs.getInt("id"));
                    this.deleteINDPartWriter.write(rsINDs.getInt("id"));
                }
                rsINDs.close();
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

        private static final long serialVersionUID = -861294530676768362L;

        private static int[] toIntArray(Column[] columns) {
            int[] intArray = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                intArray[i] = columns[i].getId();
            }
            return intArray;
        }

        int[] dependentColumns;
        int[] referencedColumns;

        public Reference(final Column[] dependentColumns, final Column[] referencedColumns) {
            this(toIntArray(dependentColumns), toIntArray(referencedColumns));
        }

        public Reference(final int[] dependentColumnIds, final int[] referencedColumnIds) {
            this.dependentColumns = dependentColumnIds;
            this.referencedColumns = referencedColumnIds;
        }

        @Override
        public IntCollection getAllTargetIds() {
            IntList allTargetIds = new IntArrayList(this.dependentColumns.length + this.referencedColumns.length);
            allTargetIds.addElements(0, this.dependentColumns);
            allTargetIds.addElements(allTargetIds.size(), this.referencedColumns);
            return allTargetIds;
        }

        /**
         * @return the dependentColumns
         */
        public int[] getDependentColumns() {
            return this.dependentColumns;
        }

        /**
         * @return the referencedColumns
         */
        public int[] getReferencedColumns() {
            return this.referencedColumns;
        }

        @Override
        public String toString() {
            return "Reference [dependentColumns=" + Arrays.toString(dependentColumns) + ", referencedColumns="
                    + Arrays.toString(referencedColumns) + "]";
        }
    }

    private static final long serialVersionUID = -932394088609862495L;
    private InclusionDependency.Reference target;

    public static InclusionDependency build(final InclusionDependency.Reference target,
            ConstraintCollection constraintCollection) {
        InclusionDependency inclusionDependency = new InclusionDependency(target, constraintCollection);
        return inclusionDependency;
    }

    public static InclusionDependency buildAndAddToCollection(final InclusionDependency.Reference target,
            ConstraintCollection constraintCollection) {
        InclusionDependency inclusionDependency = new InclusionDependency(target, constraintCollection);
        constraintCollection.add(inclusionDependency);
        return inclusionDependency;
    }

    /**
     * @see AbstractConstraint
     */
    private InclusionDependency(final InclusionDependency.Reference target,
            ConstraintCollection constraintCollection) {
        super(constraintCollection);
        if (target.dependentColumns.length != target.referencedColumns.length) {
            throw new IllegalArgumentException("Number of dependent columns must equal number of referenced columns!");
        }
        this.target = target;
    }

    @Override
    public InclusionDependency.Reference getTargetReference() {
        return target;
    }

    @Override
    public String toString() {
        return "InclusionDependency [target=" + target + "]";
    }

    public int getArity() {
        return this.getTargetReference().getDependentColumns().length;
    }

    @Override
    public ConstraintSQLSerializer<InclusionDependency> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new InclusionDependencySQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}