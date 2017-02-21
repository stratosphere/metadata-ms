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
import de.hpi.isg.mdms.model.targets.TargetReference;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

/**
 * Constraint implementation for an n-ary unique column combination.
 *
 * @author Sebastian Kruse
 */
public class DistinctValueOverlap extends AbstractHashCodeAndEquals implements RDBMSConstraint {

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

    @Deprecated
    public static DistinctValueOverlap build(final int overlap, final DistinctValueOverlap.Reference target) {
        DistinctValueOverlap uniqueColumnCombination = new DistinctValueOverlap(overlap, target);
        return uniqueColumnCombination;
    }

    public static DistinctValueOverlap buildAndAddToCollection(final int overlap,
                                                               final DistinctValueOverlap.Reference target, ConstraintCollection constraintCollection) {
        DistinctValueOverlap uniqueColumnCombination = new DistinctValueOverlap(overlap, target);
        constraintCollection.add(uniqueColumnCombination);
        return uniqueColumnCombination;
    }


    public DistinctValueOverlap(final int overlap, final DistinctValueOverlap.Reference target) {
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

}