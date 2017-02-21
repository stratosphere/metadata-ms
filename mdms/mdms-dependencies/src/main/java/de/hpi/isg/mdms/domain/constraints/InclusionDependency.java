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
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Constraint implementation for an n-ary inclusion dependency.
 *
 * @author Sebastian Kruse
 */
public class InclusionDependency extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        private static int[] toIntArray(Column[] columns) {
            int[] intArray = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                intArray[i] = columns[i].getId();
            }
            return intArray;
        }

        /**
         * Creates an IND reference and in doing so puts the column pairs in the right order.
         *
         * @param dependentColumns  are the dependent columns in the IND
         * @param referencedColumns are the referneced columns in the IND
         * @return the canonical IND reference
         */
        public static Reference sortAndBuild(final Column[] dependentColumns, final Column[] referencedColumns) {
            return sortAndBuild(toIntArray(dependentColumns), toIntArray(referencedColumns));
        }

        /**
         * Creates an IND reference and in doing so puts the column pairs in the right order.
         *
         * @param dep are the dependent column IDs in the IND
         * @param ref are the referneced column IDs in the IND
         * @return the canonical IND reference
         */
        public static Reference sortAndBuild(int[] dep, int[] ref) {
            for (int j = ref.length - 1; j > 0; j--) {
                for (int i = j - 1; i >= 0; i--) {
                    if (dep[i] > dep[j] || (dep[i] == dep[j] && ref[i] > ref[j])) {
                        swap(dep, i, j);
                        swap(ref, i, j);
                    }
                }
            }
            return new Reference(dep, ref);
        }

        private static void swap(int[] array, int i, int j) {
            int temp = array[i];
            array[i] = array[j];
            array[j] = temp;
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

        /**
         * Tests if this is a valid reference.
         *
         * @return whether it is a valid reference
         */
        public boolean isValid() {
            // Referenced and dependent side existing and similar?
            if (this.dependentColumns == null || this.referencedColumns == null || this.dependentColumns.length == 0
                    || this.dependentColumns.length != this.referencedColumns.length) {
                return false;
            }
            // Is the ordering of the IDs fulfilled?
            for (int i = 1; i < this.dependentColumns.length; i++) {
                if (this.dependentColumns[i - 1] < this.dependentColumns[i]) continue;
                if (this.dependentColumns[i - 1] > this.dependentColumns[i]) {
//            throw new IllegalArgumentException("Dependent column IDs are not in ascending order: " + Arrays.toString(dependentColumnIds));
                    return false;
                } else if (this.referencedColumns[i - 1] >= this.referencedColumns[i]) {
//            throw new IllegalArgumentException("Referenced column IDs are not in ascending order on equal dependent columns: " +
//                Arrays.toString(dependentColumnIds) + " < " + Arrays.toString(referencedColumnIds));
                    return false;
                }
            }

            return true;
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

    @Deprecated
    public static InclusionDependency build(final InclusionDependency.Reference target) {
        InclusionDependency inclusionDependency = new InclusionDependency(target);
        return inclusionDependency;
    }

    public static InclusionDependency buildAndAddToCollection(final InclusionDependency.Reference target,
                                                              ConstraintCollection<InclusionDependency> constraintCollection) {
        InclusionDependency inclusionDependency = new InclusionDependency(target);
        constraintCollection.add(inclusionDependency);
        return inclusionDependency;
    }

    public InclusionDependency(final InclusionDependency.Reference target) {
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

    /**
     * Checks whether this inclusion dependency is implied by another inclusion dependency.
     *
     * @param that is the allegedly implying IND
     * @return whether this IND is implied
     */
    public boolean isImpliedBy(InclusionDependency that) {
        if (this.getArity() > that.getArity()) {
            return false;
        }

        // Co-iterate the two INDs and make use of the sorting of the column IDs.
        int thisI = 0, thatI = 0;
        while (thisI < this.getArity() && thatI < that.getArity() && (this.getArity() - thisI <= that.getArity() - thatI)) {
            int thisCol = this.getTargetReference().dependentColumns[thisI];
            int thatCol = that.getTargetReference().dependentColumns[thatI];
            if (thisCol == thatCol) {
                thisCol = this.getTargetReference().referencedColumns[thisI];
                thatCol = that.getTargetReference().referencedColumns[thatI];
            }
            if (thisCol == thatCol) {
                thisI++;
                thatI++;
            } else if (thisCol > thatCol) {
                thatI++;
            } else {
                return false;
            }
        }

        return thisI == this.getArity();
    }

    /**
     * Tests this inclusion dependency for triviality, i.e., whether the dependent and referenced sides are equal.
     *
     * @return whether this is a trivial inclusion dependency
     */
    public boolean isTrivial() {
        return Arrays.equals(this.target.dependentColumns, this.target.referencedColumns);
    }

}