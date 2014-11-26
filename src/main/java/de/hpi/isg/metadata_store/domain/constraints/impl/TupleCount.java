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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * Constraint implementation for the number of tuples in a table.
 * 
 * @author Sebastian Kruse
 */
public class TupleCount extends AbstractConstraint {

    public static class TupleCountSQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "TupleCount";

        private final SQLInterface sqlInterface;

        public TupleCountSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;
        }

        @Override
        public void serialize(Integer constraintId, Constraint tupleCount) {
            Validate.isTrue(tupleCount instanceof TupleCount);
            try {
                Statement stmt = sqlInterface.createStatement();
                if (!allTablesExistChecked) {
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
                    if (sqlInterface.tableExists(tableName)) {
                        this.allTablesExistChecked = true;
                    }
                }

                String sqlAddTypee = String.format(
                        "INSERT INTO " + tableName + " (constraintId, tupleCount, tableId) VALUES (%d, '%s', %d);",
                        constraintId, ((TupleCount) tupleCount).getNumTuples(), tupleCount
                                .getTargetReference()
                                .getAllTargets().iterator()
                                .next().getId());
                stmt.executeUpdate(sqlAddTypee);

                stmt.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<Constraint> deserializeConstraintsForConstraintCollection(
                ConstraintCollection constraintCollection) {
            return new HashSet<>();
        }
    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Table table;

        public Reference(final Table column) {
            this.table = column;
        }

        @Override
        public Collection<Target> getAllTargets() {
            return Collections.<Target> singleton(this.table);
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
     * @see AbstractConstraint
     */
    private TupleCount(final Reference target,
            final ConstraintCollection constraintCollection, int numTuples) {

        super(constraintCollection);
        this.target = target;
        this.numTuples = numTuples;
    }

    public static TupleCount build(final Reference target, ConstraintCollection constraintCollection,
            int numTuples) {
        TupleCount tupleCount = new TupleCount(target, constraintCollection, numTuples);
        return tupleCount;
    }

    public static TupleCount buildAndAddToCollection(final Reference target,
            ConstraintCollection constraintCollection,
            int numTuples) {
        TupleCount tupleCount = new TupleCount(target, constraintCollection, numTuples);
        constraintCollection.add(tupleCount);
        return tupleCount;
    }

    @Override
    public TupleCount.Reference getTargetReference() {
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
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new TupleCountSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}