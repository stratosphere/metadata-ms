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
package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.util.Collection;
import java.util.Collections;

import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * Constraint implementation for the number of tuples in a table.
 * 
 * @author Sebastian Kruse
 */
public class TupleCount extends AbstractConstraint {

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Table table;

        public Reference(final Table column) {
            this.table = column;
        }

        @Override
        public Collection<Target> getAllTargets() {
            return Collections.<Target> singleton(this.table);
        }

        @Override
        public String toString() {
            return "Reference [table=" + table + "]";
        }

    }

    private static final long serialVersionUID = -932394088609862495L;

    private int numTuples;

    private Reference target;

    /**
     * @see AbstractConstraint
     */
    private TupleCount(final Reference target,
            final ConstraintCollection constraintCollection, int numTuples) {

        super(constraintCollection);
        this.target = target;
        this.numTuples = numTuples;
    }

    public static TupleCount build(final Reference target, ConstraintCollection constraintCollection,
            int numTuples) {
        TupleCount tupleCount = new TupleCount(target, constraintCollection, numTuples);
        return tupleCount;
    }

    public static TupleCount buildAndAddToCollection(final Reference target,
            ConstraintCollection constraintCollection,
            int numTuples) {
        TupleCount tupleCount = new TupleCount(target, constraintCollection, numTuples);
        constraintCollection.add(tupleCount);
        return tupleCount;
    }

    @Override
    public TupleCount.Reference getTargetReference() {
        return this.target;
    }

    /**
     * @return the numDistinctValues
     */
    public int getNumTuples() {
        return numTuples;
    }

    /**
     * @param numDistinctValues
     *        the numDistinctValues to set
     */
    public void setNumDistinctValues(int numDistinctValues) {
        this.numTuples = numDistinctValues;
    }

    @Override
    public String toString() {
        return "TupleCount[" + getTargetReference() + ", numTuples=" + numTuples + "]";
    }

    @Override
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        // TODO Auto-generated method stub
        return null;
    }

}