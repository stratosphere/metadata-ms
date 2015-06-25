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
import de.hpi.isg.mdms.model.constraints.AbstractConstraint;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Constraint implementation for an n-ary unique column combination.
 * 
 * @author Sebastian Kruse
 */
public class DistinctValueOverlap extends AbstractConstraint implements RDBMSConstraint {

    public static class DistinctValueOverlapSQLiteSerializer implements ConstraintSQLSerializer<DistinctValueOverlap> {

        private final static String tableName = "DistinctValueOverlap";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertWriter;

        DatabaseWriter<Integer> deleteWriter;

        DatabaseQuery<Void> queryAllConstraints;

        DatabaseQuery<Integer> queryConstraintForConstraintCollection;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintCollectionId, overlap, column1, column2) VALUES (?, ?, ?, ?);",
                        new PreparedStatementAdapter<int[]>() {
                            @Override
                            public void translateParameter(int[] parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter[0]);
                                preparedStatement.setInt(2, parameter[1]);
                                preparedStatement.setInt(3, parameter[2]);
                                preparedStatement.setInt(4, parameter[3]);
                            }
                        },
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from  " + tableName + " where constraintId = ?;",
                        new PreparedStatementAdapter<Integer>() {
                            @Override
                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter);
                            }
                        },
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> QUERY_ALL_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        ("SELECT %table%.constraintId AS constraintId, "
                        		+ "%table%.constraintCollectionId AS constraintCollectionId,"
                                + "%table%.column1 AS column1, "
                                + "%table%.column2 AS column2, "
                                + "%table%.overlap AS overlap "
                                + "FROM %table%;").replaceAll("%table%", tableName),
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> QUERY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        ("SELECT %table%.constraintId AS constraintId, "
                        		+ "%table%.constraintCollectionId AS constraintCollectionId, "
                                + "%table%.column1 AS column1, "
                                + "%table%.column2 AS column2, "
                                + "%table%.overlap AS overlap "
                                + "FROM %table% "
                                + "WHERE %table%.constraintCollectionId = ?;").replaceAll("%table%", tableName),
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public DistinctValueOverlapSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.insertWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_WRITER_FACTORY);

                this.deleteWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_WRITER_FACTORY);

                this.queryAllConstraints = sqlInterface.getDatabaseAccess().createQuery(
                        QUERY_ALL_FACTORY);

                this.queryConstraintForConstraintCollection = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                QUERY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint constraint) {

            Validate.isTrue(constraint instanceof DistinctValueOverlap);
            DistinctValueOverlap dvo = (DistinctValueOverlap) constraint;
            try {
                insertWriter.write(new int[] { dvo.getConstraintCollection().getId(), dvo.overlap, dvo.target.column1, dvo.target.column2 });
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<DistinctValueOverlap> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<DistinctValueOverlap> constraints = new HashSet<>();

            try {

                ResultSet rsDistinctValueOverlap = retrieveConstraintCollection ?
                        queryAllConstraints.execute(null) : queryConstraintForConstraintCollection
                                .execute(constraintCollection.getId());
                while (rsDistinctValueOverlap.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsDistinctValueOverlap
                                        .getInt("constraintCollectionId"));
                    }
                    int overlap = rsDistinctValueOverlap.getInt("overlap");
                    Reference reference = new Reference(rsDistinctValueOverlap.getInt("column1"),
                            rsDistinctValueOverlap.getInt("column2"));
                    constraints.add(DistinctValueOverlap.build(overlap, reference, constraintCollection));
                }
                rsDistinctValueOverlap.close();
                return constraints;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getTableNames() {
            return Collections.singletonList(tableName);
        }

        @Override
        public void initializeTables() {
            if (!sqlInterface.tableExists(tableName)) {
                String createINDTable = "CREATE TABLE [DistinctValueOverlap]"
                        + "("
                        + "    [constraintId] integer NOT NULL,"
                        + "	   [constraintCollectionId] integer NOT NULL,\n"   
                        + "    [overlap] integer NOT NULL,"
                        + "    [column1] integer NOT NULL,"
                        + "    [column2] integer NOT NULL,"
                        + "    PRIMARY KEY ([constraintId]),"
                        + "    FOREIGN KEY ([column2])"
                        + "    REFERENCES [Columnn] ([id]),"
                        + "    FOREIGN KEY ([column1])"
                        + "    REFERENCES [Columnn] ([id]),"
                        + "	   FOREIGN KEY ([constraintCollectionId])"
                        + "    REFERENCES [ConstraintCollection] ([id])"
                        + ");";
                this.sqlInterface.executeCreateTableStatement(createINDTable);
            }
            // check again and set allTablesExistChecked to true
            if (!sqlInterface.tableExists(tableName)) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            try {
                ResultSet rsINDs = queryConstraintForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsINDs.next()) {
                    this.deleteWriter.write(rsINDs.getInt("constraintId"));
                }
                rsINDs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -3272378011671591628L;

        private final int column1, column2;

        public Reference(int column1, int column2) {
            super();
            this.column1 = column1;
            this.column2 = column2;
        }

        @Override
        public IntCollection getAllTargetIds() {
            IntArrayList targetIds = new IntArrayList(2);
            targetIds.add(this.column1);
            targetIds.add(this.column2);
            return targetIds;
        }

        @Override
        public String toString() {
            return "Reference [" + column1 + ", " + column2 + "]";
        }

    }

    private static final long serialVersionUID = -932394088609862495L;

    private DistinctValueOverlap.Reference target;

    private int overlap;

    public static DistinctValueOverlap build(final int overlap, final DistinctValueOverlap.Reference target,
            ConstraintCollection constraintCollection) {
        DistinctValueOverlap uniqueColumnCombination = new DistinctValueOverlap(overlap, target, constraintCollection);
        return uniqueColumnCombination;
    }

    public static DistinctValueOverlap buildAndAddToCollection(final int overlap,
            final DistinctValueOverlap.Reference target, ConstraintCollection constraintCollection) {
        DistinctValueOverlap uniqueColumnCombination = new DistinctValueOverlap(overlap, target, constraintCollection);
        constraintCollection.add(uniqueColumnCombination);
        return uniqueColumnCombination;
    }

    /**
     * @see AbstractConstraint
     */
    private DistinctValueOverlap(final int overlap, final DistinctValueOverlap.Reference target,
            ConstraintCollection constraintCollection) {
        super(constraintCollection);
        this.overlap = overlap;
        this.target = target;
    }

    @Override
    public DistinctValueOverlap.Reference getTargetReference() {
        return target;
    }

    @Override
    public String toString() {
        return "DistinctValueOverlap [target=" + target + ", overlap=" + overlap + "]";
    }

    @Override
    public ConstraintSQLSerializer<DistinctValueOverlap> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new DistinctValueOverlapSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}