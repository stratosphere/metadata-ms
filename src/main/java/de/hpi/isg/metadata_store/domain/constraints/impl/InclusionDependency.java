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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

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
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * Constraint implementation for an n-ary inclusion dependency.
 * 
 * @author Sebastian Kruse
 */
public class InclusionDependency extends AbstractConstraint implements Constraint {

    public static class InclusionDependencySQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "IND";
        private final static String referenceTableName = "INDPart";

        private final SQLInterface sqlInterface;

        DatabaseWriter<Integer> insertInclusionDependencyWriter;

        DatabaseWriter<int[]> insertINDPartWriter;

        DatabaseQuery<Void> queryInclusionDependencies;

        DatabaseQuery<Integer> queryInclusionDependenciesForConstraintCollection;

        DatabaseQuery<Integer> queryINDPart;

        private static final PreparedStatementBatchWriter.Factory<Integer> INSERT_INCLUSIONDEPENDENCY_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId) VALUES (?);",
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

        private static final StrategyBasedPreparedQuery.Factory<Void> INCLUSIONDEPENDENCY_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                                + " from IND, constraintt where IND.constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> INCLUSIONDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                                + " from IND, constraintt where IND.constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> INDPART_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT lhs, rhs "
                                + "from INDpart "
                                + "where INDpart.constraintId = ?;",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        referenceTableName);

        public InclusionDependencySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            if (!allTablesExistChecked) {
                if (!sqlInterface.tableExists(tableName)) {
                    String createINDTable = "CREATE TABLE [" + tableName + "]\n" +
                            "(\n" +
                            "    [constraintId] integer NOT NULL,\n" +
                            "    PRIMARY KEY ([constraintId]),\n" +
                            "    FOREIGN KEY ([constraintId])\n" +
                            "    REFERENCES [Constraintt] ([id])\n" +
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
                            "    REFERENCES [IND] ([constraintId]),\n" +
                            "    FOREIGN KEY ([rhs])\n" +
                            "    REFERENCES [Columnn] ([id])\n" +
                            ");";
                    this.sqlInterface.executeCreateTableStatement(createINDpartTable);
                }
                // check again and set allTablesExistChecked to true
                if (sqlInterface.tableExists(tableName) && sqlInterface.tableExists(referenceTableName)) {
                    this.allTablesExistChecked = true;
                }
            }
            try {
                this.insertInclusionDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_INCLUSIONDEPENDENCY_WRITER_FACTORY);

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
        public void serialize(Integer constraintId, Constraint inclusionDependency) {

            Validate.isTrue(inclusionDependency instanceof InclusionDependency);
            try {
                insertInclusionDependencyWriter.write(constraintId);

                for (int i = 0; i < ((InclusionDependency) inclusionDependency).getArity(); i++) {
                    insertINDPartWriter.write(new int[] { constraintId,
                            ((InclusionDependency) inclusionDependency).getTargetReference()
                                    .getDependentColumns()[i].getId(),
                            ((InclusionDependency) inclusionDependency).getTargetReference()
                                    .getReferencedColumns()[i].getId() });
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<Constraint> deserializeConstraintsForConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<Constraint> inclusionDependencies = new HashSet<>();

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
    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Column[] dependentColumns;
        Column[] referencedColumns;

        public Reference(final Column[] dependentColumns, final Column[] referencedColumns) {
            this.dependentColumns = dependentColumns;
            this.referencedColumns = referencedColumns;
        }

        @Override
        public Collection<Target> getAllTargets() {
            final List<Target> result = new ArrayList<>(this.dependentColumns.length + this.referencedColumns.length);
            result.addAll(Arrays.asList(this.dependentColumns));
            result.addAll(Arrays.asList(this.referencedColumns));
            return result;
        }

        /**
         * @return the dependentColumns
         */
        public Column[] getDependentColumns() {
            return this.dependentColumns;
        }

        /**
         * @return the referencedColumns
         */
        public Column[] getReferencedColumns() {
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
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new InclusionDependencySQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}