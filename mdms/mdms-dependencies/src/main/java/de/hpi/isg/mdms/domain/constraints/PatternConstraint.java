package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.targets.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This class is a {@link de.hpi.isg.mdms.model.constraints.Constraint} representing the pattern of a certain {@link Column}. {@link Column}.
 */
public class PatternConstraint extends AbstractHashCodeAndEquals implements Constraint {

    private final HashMap<String, Integer> patterns;

    private int columnId;

    public PatternConstraint(int columnId, HashMap<String, Integer> patterns) {
        this.patterns = patterns;
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
        return "PatternConstraint[pattern=" + patterns + "]";
    }

    public HashMap<String, Integer> getPatterns() {
        return patterns;
    }

}
