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
import java.util.HashSet;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
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

        public DistinctValueCountSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;
        }

        @Override
        public void serialize(Integer constraintId, Constraint distinctValueCount) {
            Validate.isTrue(distinctValueCount instanceof DistinctValueCount);
            try {
                Statement stmt = sqlInterface.createStatement();
                if (!allTablesExistChecked) {
                    if (!sqlInterface.tableExists(tableName)) {
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

                String sqlAddTypee = String
                        .format(
                                "INSERT INTO " + tableName
                                        + " (constraintId, distinctValueCount, columnId) VALUES (%d, '%d', %d);",
                                constraintId, ((DistinctValueCount) distinctValueCount).getNumDistinctValues(),
                                distinctValueCount
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