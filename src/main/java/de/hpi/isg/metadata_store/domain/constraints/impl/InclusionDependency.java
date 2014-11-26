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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * Constraint implementation for an n-ary inclusion dependency.
 * 
 * @author Sebastian Kruse
 */
public class InclusionDependency extends AbstractConstraint implements Constraint {

    private class InclusionDependencySQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "Typee";
        private final static String referenceTableName = "Typee";

        private final SQLInterface sqlInterface;

        public InclusionDependencySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;
        }

        @Override
        public void serialize(Integer constraintId, Constraint inclusionDependency) {

            Validate.isTrue(inclusionDependency instanceof InclusionDependency);
            try {
                Statement stmt = sqlInterface.createStatement();
                if (!allTablesExistChecked) {
                    if (!sqlInterface.tableExists(tableName)) {
                        String createINDTable = "CREATE TABLE [IND]\n" +
                                "(\n" +
                                "    [constraintId] integer NOT NULL,\n" +
                                "    PRIMARY KEY ([constraintId]),\n" +
                                "    FOREIGN KEY ([constraintId])\n" +
                                "    REFERENCES [Constraintt] ([id])\n" +
                                ");";
                        this.sqlInterface.executeCreateTableStatement(createINDTable);
                    }
                    if (!sqlInterface.tableExists(referenceTableName)) {
                        String createINDpartTable = "CREATE TABLE [INDpart]\n" +
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

                String sqlAddIND = String.format(
                        "INSERT INTO IND (constraintId) VALUES (%d);", constraintId);
                stmt.executeUpdate(sqlAddIND);
                for (int i = 0; i < ((InclusionDependency) inclusionDependency).getArity(); i++) {
                    String sqlAddINDpart = String
                            .format(
                                    "INSERT INTO INDpart (constraintId, lhs, rhs) VALUES ('%d', %d, %d);",
                                    constraintId,
                                    ((InclusionDependency) inclusionDependency).getTargetReference()
                                            .getDependentColumns()[i].getId(),
                                    ((InclusionDependency) inclusionDependency).getTargetReference()
                                            .getReferencedColumns()[i].getId());
                    stmt.executeUpdate(sqlAddINDpart);
                }
                stmt.close();
            } catch (SQLException e) {
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