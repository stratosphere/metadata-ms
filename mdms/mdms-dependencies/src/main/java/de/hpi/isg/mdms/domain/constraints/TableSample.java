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
import de.hpi.isg.mdms.model.targets.Table;

/**
 * This {@link Constraint} represents a random sample of tuples from a table.
 *
 * @author Sebastian Kruse
 */
public class TableSample extends AbstractHashCodeAndEquals implements Constraint {

    private int tableId;

    private String[][] tuples;

    public TableSample(int tableId, String[][] tuples) {
        this.tableId = tableId;
        this.tuples = tuples;
    }

    /**
     * Retrieve the tuples of this instance.
     *
     * @return the sampled tuples
     */
    public String[][] getTuples() {
        return this.tuples;
    }

    /**
     * Set the tuples of this instance.
     *
     * @param tuples the sampled tuples
     */
    public void setTuples(String[][] tuples) {
        this.tuples = tuples;
    }

    /**
     * @return the ID of the {@link Table} being described
     */
    public int getTableId() {
        return this.tableId;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.tableId};
    }

}