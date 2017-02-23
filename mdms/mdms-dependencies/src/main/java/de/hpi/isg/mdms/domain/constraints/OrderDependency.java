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
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.util.ReferenceUtils;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;

/**
 * {@link Constraint} implementation for an n-ary order dependency.
 *
 * @author Sebastian Kruse
 */
public class OrderDependency extends AbstractHashCodeAndEquals implements Constraint {


    private final int[] determinantColumns, dependentColumns;

    public OrderDependency(int dependentColumnId, int referencedColumnId) {
        this(new int[]{dependentColumnId}, new int[]{referencedColumnId});
    }

    public OrderDependency(int[] determinantColumns, int[] dependentColumns) {
        Validate.isTrue(determinantColumns.length == dependentColumns.length);
        Validate.isTrue(ReferenceUtils.isSorted(determinantColumns));
        this.determinantColumns = determinantColumns;
        this.dependentColumns = dependentColumns;
    }

    public int[] getDeterminantColumns() {
        return this.determinantColumns;
    }

    public int[] getDependentColumns() {
        return this.dependentColumns;
    }

    @Override
    public int[] getAllTargetIds() {
        int arity = this.getArity();
        int[] allIds = new int[arity * 2];
        System.arraycopy(this.determinantColumns, 0, allIds, 0, arity);
        System.arraycopy(this.dependentColumns, 0, allIds, arity, arity);
        return allIds;
    }

    @Override
    public String toString() {
        return String.format("%s \u2286 %s", Arrays.toString(this.determinantColumns), Arrays.toString(this.dependentColumns));
    }

    public int getArity() {
        return this.determinantColumns.length;
    }

    /**
     * Checks whether this instance is implied by another instance.
     *
     * @param that is the allegedly implying instance
     * @return whether this instance is implied
     */
    public boolean isImpliedBy(OrderDependency that) {
        if (this.getArity() > that.getArity()) {
            return false;
        }

        // Co-iterate the two INDs and make use of the sorting of the column IDs.
        int thisI = 0, thatI = 0;
        while (thisI < this.getArity() && thatI < that.getArity() && (this.getArity() - thisI <= that.getArity() - thatI)) {
            int thisCol = this.determinantColumns[thisI];
            int thatCol = that.determinantColumns[thatI];
            if (thisCol == thatCol) {
                thisCol = this.dependentColumns[thisI];
                thatCol = that.dependentColumns[thatI];
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
        return Arrays.equals(this.determinantColumns, this.dependentColumns);
    }

}