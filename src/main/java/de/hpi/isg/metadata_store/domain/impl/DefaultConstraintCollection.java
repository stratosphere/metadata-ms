package de.hpi.isg.metadata_store.domain.impl;

import java.util.Collection;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

public class DefaultConstraintCollection extends AbstractHashCodeAndEquals implements ConstraintCollection {

    private final Set<Constraint> constraints;
    private final Set<Target> scope;

    public DefaultConstraintCollection(Set<Constraint> constraints, Set<Target> scope) {
        this.constraints = constraints;
        this.scope = scope;
    }

    @Override
    public Collection<Constraint> getConstraints() {
        return this.constraints;
    }

    @Override
    public Collection<Target> getScope() {
        return this.scope;
    }

}
