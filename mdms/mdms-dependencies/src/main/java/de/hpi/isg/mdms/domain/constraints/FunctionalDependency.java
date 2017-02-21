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
 * Constraint implementation for a functional dependency.
 */
public class FunctionalDependency extends AbstractHashCodeAndEquals implements Constraint {

    private final int[] lhsColumnIds;

    private final int rhsColumnId;

    public FunctionalDependency(int[] lhs, int rhs) {
        Validate.isTrue(ReferenceUtils.isSorted(lhs));
        this.lhsColumnIds = lhs;
        this.rhsColumnId = rhs;
    }

    public int getArity() {
        return this.lhsColumnIds.length;
    }

    public int[] getLhsColumnIds() {
        return this.lhsColumnIds;
    }

    public int getRhsColumnId() {
        return this.rhsColumnId;
    }

    @Override
    public int[] getAllTargetIds() {
        int[] allIds = Arrays.copyOf(this.lhsColumnIds, lhsColumnIds.length + 1);
        allIds[allIds.length - 1] = this.rhsColumnId;
        return allIds;
    }

    @Override
    public String toString() {
        return String.format("%s \u2192 %d", Arrays.toString(this.lhsColumnIds), this.rhsColumnId);
    }
}