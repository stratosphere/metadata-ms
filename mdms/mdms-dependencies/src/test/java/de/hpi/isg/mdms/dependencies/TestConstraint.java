package de.hpi.isg.mdms.dependencies;

import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.targets.Column;

/**
 * A simple dummy constraint for general metadata store tests for the
 *
 * @author Sebastian
 * @since 05.03.2015.
 */
public class TestConstraint implements Constraint {

    private final int columnId1, columnId2;

    public TestConstraint(Column column1, Column column2) {
        this.columnId1 = column1.getId();
        this.columnId2 = column2.getId();
    }

    @Override
    public int[] getAllTargetIds() {
        return new int[]{this.columnId1, this.columnId2};
    }
}
