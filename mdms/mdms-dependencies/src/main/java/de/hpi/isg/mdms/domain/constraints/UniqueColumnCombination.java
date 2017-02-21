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

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.TargetReference;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.util.Arrays;

/**
 * Constraint implementation for an n-ary unique column combination.
 *
 * @author Sebastian Kruse
 */
public class UniqueColumnCombination extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -3272378011671591628L;

        private static int[] toIntArray(Column[] columns) {
            int[] intArray = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                intArray[i] = columns[i].getId();
            }
            return intArray;
        }

        int[] uniqueColumns;

        public Reference(final Column[] uniqueColumns) {
            this(toIntArray(uniqueColumns));
        }

        public Reference(final int[] uniqueColumns) {
            this.uniqueColumns = uniqueColumns;
            Arrays.sort(this.uniqueColumns);
        }

        @Override
        public IntCollection getAllTargetIds() {
            return new IntArrayList(this.uniqueColumns);
        }

        @Override
        public String toString() {
            return "Reference [uniqueColumns=" + Arrays.toString(uniqueColumns) + "]";
        }
    }

    private static final long serialVersionUID = -932394088609862495L;
    private UniqueColumnCombination.Reference target;

    @Deprecated
    public static UniqueColumnCombination build(final UniqueColumnCombination.Reference target,
                                                ConstraintCollection constraintCollection) {
        UniqueColumnCombination uniqueColumnCombination = new UniqueColumnCombination(target);
        return uniqueColumnCombination;
    }

    public static UniqueColumnCombination buildAndAddToCollection(final UniqueColumnCombination.Reference target,
                                                                  ConstraintCollection<UniqueColumnCombination> constraintCollection) {
        UniqueColumnCombination uniqueColumnCombination = new UniqueColumnCombination(target);
        constraintCollection.add(uniqueColumnCombination);
        return uniqueColumnCombination;
    }


    public UniqueColumnCombination(final UniqueColumnCombination.Reference target) {
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
        return this.getTargetReference().uniqueColumns.length;
    }

}