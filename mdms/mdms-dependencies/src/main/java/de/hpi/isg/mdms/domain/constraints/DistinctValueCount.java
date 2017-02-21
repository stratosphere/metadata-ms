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
 * {@link Constraint} implementation distinct value counts of a single {@link Column}.
 *
 * @author Sebastian Kruse
 */
public class DistinctValueCount extends AbstractHashCodeAndEquals implements Constraint {

    private int columnId;

    private int numDistinctValues;

    public DistinctValueCount(int columnId, int numDistinctValues) {
        this.columnId = columnId;
        this.numDistinctValues = numDistinctValues;
    }

    /**
     * @return the numDistinctValues
     */
    public int getNumDistinctValues() {
        return this.numDistinctValues;
    }

    /**
     * @return the ID of the {@link Column} being described
     */
    public int getColumnId() {
        return this.columnId;
    }

    /**
     * @param numDistinctValues the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numDistinctValues = numDistinctValues;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.columnId};
    }

    @Override
    public String toString() {
        return String.format("%s[col=%08x, %,d]",
                this.getClass().getSimpleName(), this.columnId, this.numDistinctValues
        );
    }

}