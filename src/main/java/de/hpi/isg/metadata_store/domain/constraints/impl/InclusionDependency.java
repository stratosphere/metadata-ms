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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
public class InclusionDependency extends AbstractConstraint {

    public static class Reference implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        Column[] dependentColumns;
        Column[] referencedColumns;

        public Reference(final Column[] dependentColumns, final Column[] referencedColumns) {
            this.dependentColumns = dependentColumns;
            this.referencedColumns = referencedColumns;
        }

        @Override
        public Collection<Target> getAllTargets() {
            final List<Target> result = new ArrayList<>(this.dependentColumns.length + this.referencedColumns.length);
            result.addAll(Arrays.asList(this.dependentColumns));
            result.addAll(Arrays.asList(this.referencedColumns));
            return result;
        }

        /**
         * @return the dependentColumns
         */
        public Column[] getDependentColumns() {
            return this.dependentColumns;
        }

        /**
         * @return the referencedColumns
         */
        public Column[] getReferencedColumns() {
            return this.referencedColumns;
        }
    }

    private static final long serialVersionUID = -932394088609862495L;

    /**
     * @see AbstractConstraint
     */
    public InclusionDependency(final Observer observer, final int id, final String name, final TargetReference target) {
        super(observer, id, target);

    }

    @Override
    public InclusionDependency.Reference getTargetReference() {
        return (InclusionDependency.Reference) super.getTargetReference();
    }

    public int getArity() {
        return this.getTargetReference().getDependentColumns().length;
    }

}