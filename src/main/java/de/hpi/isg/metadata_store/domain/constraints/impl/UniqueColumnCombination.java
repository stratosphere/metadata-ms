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
 * Constraint implementation for an n-ary unique column combination.
 * 
 * @author Sebastian Kruse
 */
public class UniqueColumnCombination extends AbstractConstraint implements Constraint {

    public static class UniqueColumnCombinationSQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "UCC";
        private final static String referenceTableName = "UCCPart";

        private final SQLInterface sqlInterface;

        DatabaseWriter<Integer> insertUniqueColumnCombinationWriter;

        DatabaseWriter<int[]> insertUCCPartWriter;

        DatabaseQuery<Void> queryUniqueColumnCombination;

        DatabaseQuery<Integer> queryUniqueColumnCombinationsForConstraintCollection;

        DatabaseQuery<Integer> queryUCCPart;

        private static final PreparedStatementBatchWriter.Factory<Integer> INSERT_UNIQECOLUMNCOMBINATION_WRITER_FACTORY =
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

        private static final StrategyBasedPreparedQuery.Factory<Void> UNIQECOLUMNCOMBINATION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + ", constraintt where " + tableName
                                + ".constraintId = constraintt.id;",
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> UNIQECOLUMNCOMBINATION_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        "SELECT constraintt.id as id, constraintt.constraintCollectionId as constraintCollectionId"
                                + " from " + tableName + ", constraintt where " + tableName
                                + ".constraintId = constraintt.id"
                                + " and constraintt.constraintCollectionId=?;",
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
                            "    [col] integer NOT NULL,\n" +
                            "    FOREIGN KEY ([col])\n" +
                            "    REFERENCES [Columnn] ([id]),\n" +
                            "    FOREIGN KEY ([constraintId])\n" +
                            "    REFERENCES [" + tableName + "] ([constraintId])" +
                            ");";
                    this.sqlInterface.executeCreateTableStatement(createINDpartTable);
                }
                // check again and set allTablesExistChecked to true
                if (sqlInterface.tableExists(tableName) && sqlInterface.tableExists(referenceTableName)) {
                    this.allTablesExistChecked = true;
                }
            }
            try {
                this.insertUniqueColumnCombinationWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_UNIQECOLUMNCOMBINATION_WRITER_FACTORY);

                this.insertUCCPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_UCCPART_WRITER_FACTORY);

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
        public void serialize(Integer constraintId, Constraint uniqueColumnCombination) {

            Validate.isTrue(uniqueColumnCombination instanceof UniqueColumnCombination);
            try {
                insertUniqueColumnCombinationWriter.write(constraintId);

                for (int i = 0; i < ((UniqueColumnCombination) uniqueColumnCombination).getArity(); i++) {
                    insertUCCPartWriter.write(new int[] { constraintId,
                            ((UniqueColumnCombination) uniqueColumnCombination).getTargetReference()
                                    .getUniqueColumns()[i].getId() });
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<Constraint> deserializeConstraintsForConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<Constraint> uniqueColumnCombiantions = new HashSet<>();

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
            List<Column> cols = new ArrayList<>();
            try {
                try (ResultSet rs = this.queryUCCPart.execute(id);) {
                    while (rs.next()) {
                        cols.add(this.sqlInterface.getColumnById(rs.getInt("col")));
                    }
                }
                return new Reference(cols.toArray(new Column[cols.size()]));
            } catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -3272378011671591628L;

        Column[] uniqueColumns;

        public Reference(final Column[] uniqueColumns) {
            this.uniqueColumns = uniqueColumns;
        }

        @Override
        public Collection<Target> getAllTargets() {
            final List<Target> result = new ArrayList<>(this.uniqueColumns.length);
            result.addAll(Arrays.asList(this.uniqueColumns));
            return result;
        }

        /**
         * @return the unique columns
         */
        public Column[] getUniqueColumns() {
            return this.uniqueColumns;
        }

        @Override
        public String toString() {
            return "Reference [uniqueColumns=" + Arrays.toString(uniqueColumns) + "]";
        }
    }

    private static final long serialVersionUID = -932394088609862495L;
    private UniqueColumnCombination.Reference target;

    public static UniqueColumnCombination build(final UniqueColumnCombination.Reference target,
            ConstraintCollection constraintCollection) {
        UniqueColumnCombination uniqueColumnCombination = new UniqueColumnCombination(target, constraintCollection);
        return uniqueColumnCombination;
    }

    public static UniqueColumnCombination buildAndAddToCollection(final UniqueColumnCombination.Reference target,
            ConstraintCollection constraintCollection) {
        UniqueColumnCombination uniqueColumnCombination = new UniqueColumnCombination(target, constraintCollection);
        constraintCollection.add(uniqueColumnCombination);
        return uniqueColumnCombination;
    }

    /**
     * @see AbstractConstraint
     */
    private UniqueColumnCombination(final UniqueColumnCombination.Reference target,
            ConstraintCollection constraintCollection) {
        super(constraintCollection);
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
        return this.getTargetReference().getUniqueColumns().length;
    }

    @Override
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new UniqueColumnCombinationSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}