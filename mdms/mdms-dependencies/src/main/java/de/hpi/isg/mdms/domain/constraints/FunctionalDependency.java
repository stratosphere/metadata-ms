/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.TargetReference;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.util.Arrays;

/**
 * Constraint implementation for a functional dependency.
 */
public class FunctionalDependency extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -3272378011671591628L;

        private static int[] toIntArray(Column[] columns) {
            int[] intArray = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                intArray[i] = columns[i].getId();
            }
            return intArray;
        }

        int[] lhs_columns;
        int rhs_column;

        public Reference(final Column rhs_column, final Column[] lhs_columns) {
            this.lhs_columns = toIntArray(lhs_columns);
            Arrays.sort(this.lhs_columns);
            this.rhs_column = rhs_column.getId();
        }

        public Reference(final int rhs_column, final int[] lhs_columns) {
            this.lhs_columns = lhs_columns;
            Arrays.sort(this.lhs_columns);
            this.rhs_column = rhs_column;
        }

        public IntCollection getLHSTargetIds() {
            return new IntArrayList(this.lhs_columns);
        }

        public int getRHSTarget() {
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
                                                               ConstraintCollection<FunctionalDependency> constraintCollection) {
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

}