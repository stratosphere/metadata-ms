package de.hpi.isg.mdms.model.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;

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

}