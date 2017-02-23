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
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.targets.Column;

/**
 * {@link Constraint} implementation to store the overlap of distinct values of two {@link Column}s.
 *
 * @author Sebastian Kruse
 */
public class DistinctValueOverlap extends AbstractHashCodeAndEquals implements Constraint {

    private final int columnId1, columnId2;

    private int overlap;

    public DistinctValueOverlap(final int overlap, final int columnId1, final int columnId2) {
        this.overlap = overlap;
        this.columnId1 = columnId1;
        this.columnId2 = columnId2;
    }

    public int getColumnId1() {
        return this.columnId1;
    }

    public int getColumnId2() {
        return this.columnId2;
    }

    public int getOverlap() {
        return this.overlap;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.columnId1, this.columnId2};
    }

    @Override
    public String toString() {
        return String.format("%s[col1=%08x, col2=%08x, %,d]",
                this.getClass().getSimpleName(), this.columnId1, this.columnId2, this.overlap
        );
    }
}