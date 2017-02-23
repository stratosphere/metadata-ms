package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a {@link de.hpi.isg.mdms.model.constraints.Constraint} representing the data type of a certain {@link Column}. {@link Column}.
 */
public class TypeConstraint extends AbstractHashCodeAndEquals implements Constraint {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeConstraint.class);

    private static final long serialVersionUID = 3194245498846860560L;

    private final String type;

    private final int columnId;

    public TypeConstraint(final int columnId, String type) {
        this.columnId = columnId;
        this.type = type;
    }

    public int getColumnId() {
        return this.columnId;
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.columnId};
    }

    @Override
    public String toString() {
        return "TypeConstraint [type=" + type + "]";
    }

    public String getType() {
        return type;
    }

}
