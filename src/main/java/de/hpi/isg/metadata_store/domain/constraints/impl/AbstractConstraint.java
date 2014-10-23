package de.hpi.isg.metadata_store.domain.constraints.impl;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiable;

public abstract class AbstractConstraint extends AbstractIdentifiable implements Constraint {

    private static final long serialVersionUID = 8774433936666609976L;
    private final ConstraintCollection constraintCollection;

    public AbstractConstraint(int id, ConstraintCollection constraintCollection) {
        super(id);
        this.constraintCollection = constraintCollection;
    }

    @Override
    public ConstraintCollection getConstraintCollection() {
        return constraintCollection;
    }

}
