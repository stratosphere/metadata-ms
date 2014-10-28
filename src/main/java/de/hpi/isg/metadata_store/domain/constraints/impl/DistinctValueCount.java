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

import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.impl.AbstractConstraint;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * Constraint implementation for an n-ary inclusion dependency.
 * 
 * @author Sebastian Kruse
 */
public class DistinctValueCount extends AbstractConstraint {

    public static class Reference implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Column column;

        public Reference(final Column column) {
            this.column = column;
        }

        @Override
        public Collection<Target> getAllTargets() {
            return Collections.<Target>singleton(this.column);
        }

        @Override
        public String toString() {
            return "Reference [column=" + column + "]";
        }
        

    }

    private static final long serialVersionUID = -932394088609862495L;
    
    private int numDistinctValues;

    /**
     * @see AbstractConstraint
     */
    public DistinctValueCount(final Observer observer, final int id, final String name, final TargetReference target, int numDistinctValues) {
        super(observer, id, target);
        this.numDistinctValues = numDistinctValues;
    }

    @Override
    public DistinctValueCount.Reference getTargetReference() {
        return (DistinctValueCount.Reference) super.getTargetReference();
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