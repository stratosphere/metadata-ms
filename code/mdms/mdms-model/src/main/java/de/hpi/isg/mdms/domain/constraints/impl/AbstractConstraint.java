package de.hpi.isg.mdms.domain.constraints.impl;

import de.hpi.isg.mdms.domain.Constraint;
import de.hpi.isg.mdms.domain.ConstraintCollection;
import de.hpi.isg.mdms.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.domain.common.impl.ExcludeHashCodeEquals;

/**
 * This class provided basic functionality for all {@link Constraint}s. For example it stores the
 * {@link ConstraintCollection} it belongs to.
 * 
 * @author fabian
 *
 */

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
