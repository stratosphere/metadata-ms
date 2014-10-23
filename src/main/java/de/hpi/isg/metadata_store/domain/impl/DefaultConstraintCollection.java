package de.hpi.isg.metadata_store.domain.impl;

import java.util.Collection;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiable;

public class DefaultConstraintCollection extends AbstractIdentifiable implements ConstraintCollection {

    private static final long serialVersionUID = -6633086023388829925L;
    private final Set<Constraint> constraints;
    private final Set<Target> scope;

    public DefaultConstraintCollection(int id, Set<Constraint> constraints, Set<Target> scope) {
        super(id);
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
