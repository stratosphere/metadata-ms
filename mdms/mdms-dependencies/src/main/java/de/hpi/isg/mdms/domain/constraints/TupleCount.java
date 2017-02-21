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
 * Constraint implementation for the number of tuples in a table.
 *
 * @author Sebastian Kruse
 */
public class TupleCount extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final long serialVersionUID = -932394088609862495L;

    private int numTuples;

    private SingleTargetReference target;

    public TupleCount(final SingleTargetReference target, int numTuples) {

        this.target = target;
        this.numTuples = numTuples;
    }

    @Deprecated
    public static TupleCount build(final SingleTargetReference target, ConstraintCollection<TupleCount> constraintCollection,
                                   int numTuples) {
        return new TupleCount(target, numTuples);
    }

    public static TupleCount buildAndAddToCollection(final SingleTargetReference target,
                                                     ConstraintCollection<TupleCount> constraintCollection,
                                                     int numTuples) {
        TupleCount tupleCount = new TupleCount(target, numTuples);
        constraintCollection.add(tupleCount);
        return tupleCount;
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.target;
    }

    /**
     * @return the numDistinctValues
     */
    public int getNumTuples() {
        return numTuples;
    }

    /**
     * @param numDistinctValues the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numTuples = numDistinctValues;
    }

    @Override
    public String toString() {
        return "TupleCount[" + getTargetReference() + ", numTuples=" + numTuples + "]";
    }

}