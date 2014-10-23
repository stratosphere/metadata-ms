package de.hpi.isg.metadata_store.domain.constraints.impl;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiable;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * This class is a {@link Constraint} representing the data type of a certain {@link Column}. {@link Column}.
 */
public class TypeConstraint extends AbstractConstraint implements Constraint {

    public enum TYPES {
        STRING, INTEGER, DECIMAL
    };

    private static final long serialVersionUID = 3194245498846860560L;

    private final TYPES type;

    private final TargetReference target;

    public TypeConstraint(final int id, final SingleTargetReference target, TYPES type,
            ConstraintCollection constraintCollection) {
        super(id, constraintCollection);
        Validate.isTrue(target.getAllTargets().size() == 1);
        for (final Target t : target.getAllTargets()) {
            if (!(t instanceof Column)) {
                throw new IllegalArgumentException("TypeConstrains can only be defined on Columns.");
            }
        }
        this.type = type;
        this.target = target;
    }

    @Override
    public String toString() {
        return "TypeConstraint [type=" + type + ", getId()=" + getId() + "]";
    }

    @Override
    public TargetReference getTargetReference() {
        return target;
    }

    public TYPES getType() {
        return type;
    }
}
