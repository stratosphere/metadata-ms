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
 * This {@link Constraint} represents a {@link Column} as a signature (e.g., a min-hash signature).
 *
 * @author Sebastian Kruse
 */
public class Signature extends AbstractHashCodeAndEquals implements Constraint {

    private int columnId;

    private int[] values;

    public Signature(int columnId, int[] values) {
        this.columnId = columnId;
        this.values = values;
    }

    /**
     * Retrieve the values of this instance.
     *
     * @return the vector values
     */
    public int[] getValues() {
        return this.values;
    }

    /**
     * Set the values of this instance.
     *
     * @param values the vector values
     */
    public void setValues(int[] values) {
        this.values = values;
    }

    /**
     * @return the ID of the {@link Column} being described
     */
    public int getColumnId() {
        return this.columnId;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.columnId};
    }

    @Override
    public String toString() {
        return String.format("%s[col=%08x, %,d dimensions]",
                this.getClass().getSimpleName(), this.columnId, this.values == null ? -1 : this.values.length
        );
    }

}