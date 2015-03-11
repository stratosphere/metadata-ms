package de.hpi.isg.mdms.domain;

import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.model.constraints.AbstractConstraint;
import de.hpi.isg.mdms.model.targets.Column;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

/**
 * A simple dummy constraint for general metadata store tests for the
 *
 * @author Sebastian
 * @since 05.03.2015.
 */
public class TestConstraint extends AbstractConstraint {

    private final TestReference reference;

    public TestConstraint(ConstraintCollection constraintCollection, Column column1, Column column2) {
        super(constraintCollection);
        this.reference = new TestReference(column1, column2);
    }

    @Override
    public TargetReference getTargetReference() {
        return this.reference;
    }

    public static class TestReference implements TargetReference {

        private final Column column1, column2;

        public TestReference(Column column1, Column column2) {
            this.column1 = column1;
            this.column2 = column2;
        }

        @Override
        public IntCollection getAllTargetIds() {
            return new IntArrayList(new int[] { this.column1.getId(), this.column2.getId() });
        }
    }


}
