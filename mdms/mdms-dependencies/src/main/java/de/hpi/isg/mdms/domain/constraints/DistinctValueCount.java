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

/**
 * Constraint implementation distinct value counts of a single column.
 *
 * @author Sebastian Kruse
 */
public class DistinctValueCount extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final long serialVersionUID = -932394088609862495L;

    private int numDistinctValues;

    private SingleTargetReference target;

    public DistinctValueCount(final SingleTargetReference target, int numDistinctValues) {

        this.target = target;
        this.numDistinctValues = numDistinctValues;
    }

    @Deprecated
    public static DistinctValueCount build(final SingleTargetReference target,
                                           int numDistinctValues) {
        DistinctValueCount distinctValueCount = new DistinctValueCount(target, numDistinctValues);
        return distinctValueCount;
    }

    public static DistinctValueCount buildAndAddToCollection(final SingleTargetReference target,
                                                             ConstraintCollection<DistinctValueCount> constraintCollection,
                                                             int numDistinctValues) {
        DistinctValueCount distinctValueCount = new DistinctValueCount(target, numDistinctValues);
        constraintCollection.add(distinctValueCount);
        return distinctValueCount;
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.target;
    }

    /**
     * @return the numDistinctValues
     */
    public int getNumDistinctValues() {
        return numDistinctValues;
    }

    /**
     * @param numDistinctValues the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numDistinctValues = numDistinctValues;
    }

    @Override
    public String toString() {
        return "DistinctValueCount[" + getTargetReference() + ", numDistinctValues=" + numDistinctValues + "]";
    }

}