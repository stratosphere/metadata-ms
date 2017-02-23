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

/**
 * Constraint implementation for the number of tuples in a table.
 *
 * @author Sebastian Kruse
 */
public class TupleCount extends AbstractHashCodeAndEquals implements Constraint {

    private static final long serialVersionUID = -932394088609862495L;

    private int numTuples;

    private int tableId;

    public TupleCount(final int tableId, int numTuples) {
        this.tableId = tableId;
        this.numTuples = numTuples;
    }

    /**
     * @return the numDistinctValues
     */
    public int getNumTuples() {
        return numTuples;
    }

    public int getTableId() {
        return this.tableId;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.tableId};
    }

    /**
     * @param numDistinctValues the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numTuples = numDistinctValues;
    }

}