package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.targets.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a {@link Constraint} stating that a certain {@link Column} is not nullable.
 */
public class NotNullConstraint extends AbstractHashCodeAndEquals implements Constraint {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotNullConstraint.class);

    private static final long serialVersionUID = 3194245498846860560L;

    private final int columnId;

    public NotNullConstraint(final int columnId) {
        this.columnId = columnId;
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
        return "Not Null [ID=" + this.columnId + "]";
    }

}
