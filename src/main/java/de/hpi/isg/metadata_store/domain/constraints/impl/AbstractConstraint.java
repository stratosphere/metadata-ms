package de.hpi.isg.metadata_store.domain.constraints.impl;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;

public abstract class AbstractConstraint extends AbstractHashCodeAndEquals implements Constraint {

    private static final long serialVersionUID = 8774433936666609976L;

    @ExcludeHashCodeEquals
    private final ConstraintCollection constraintCollection;

    public AbstractConstraint(ConstraintCollection constraintCollection) {
        this.constraintCollection = constraintCollection;
    }

    @Override
    public ConstraintCollection getConstraintCollection() {
        return constraintCollection;
    }
}
