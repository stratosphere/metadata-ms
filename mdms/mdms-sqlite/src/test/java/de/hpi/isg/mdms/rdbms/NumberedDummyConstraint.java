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
package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.domain.constraints.RDBMSConstraint;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.targets.TargetReference;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntLists;

/**
 * Constraint implementation for the number of tuples in a target.
 *
 * @author Sebastian Kruse
 */
public class NumberedDummyConstraint extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Target target;

        public Reference(final Target target) {
            this.target = target;
        }

        @Override
        public IntCollection getAllTargetIds() {
            return IntLists.singleton(this.target.getId());
        }

        @Override
        public String toString() {
            return "Reference [target=" + target + "]";
        }

    }

    private static final long serialVersionUID = -932394088609862495L;

    private int value;

    private Reference target;

    private NumberedDummyConstraint(final Reference target, int value) {

        this.target = target;
        this.value = value;
    }

    public static NumberedDummyConstraint build(final Reference target, int numTuples) {
        NumberedDummyConstraint dummy = new NumberedDummyConstraint(target, numTuples);
        return dummy;
    }

    public static NumberedDummyConstraint buildAndAddToCollection(final Reference target,
                                                                  ConstraintCollection constraintCollection,
                                                                  int numTuples) {
        NumberedDummyConstraint dummy = new NumberedDummyConstraint(target, numTuples);
        constraintCollection.add(dummy);
        return dummy;
    }

    public static NumberedDummyConstraint buildAndAddToCollection(final Target target,
                                                                  ConstraintCollection constraintCollection,
                                                                  int numTuples) {
        return buildAndAddToCollection(new NumberedDummyConstraint.Reference(target), constraintCollection, numTuples);
    }

    @Override
    public NumberedDummyConstraint.Reference getTargetReference() {
        return this.target;
    }

    /**
     * @return the numDistinctValues
     */
    public int getValue() {
        return value;
    }

    /**
     * @param numDistinctValues the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.value = numDistinctValues;
    }

    @Override
    public String toString() {
        return "dummy[" + getTargetReference() + ", value=" + value + "]";
    }

}