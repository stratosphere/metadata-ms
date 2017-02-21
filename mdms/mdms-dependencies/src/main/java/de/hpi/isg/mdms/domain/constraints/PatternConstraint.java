package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This class is a {@link de.hpi.isg.mdms.model.constraints.Constraint} representing the pattern of a certain {@link Column}. {@link Column}.
 */
public class PatternConstraint extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatternConstraint.class);

    private static final long serialVersionUID = 3194245498846860560L;

    private final HashMap<String, Integer> patterns;

    private final TargetReference target;

    @Deprecated
    public static PatternConstraint build(final SingleTargetReference target,
                                          ConstraintCollection constraintCollection,
                                          HashMap<String, Integer> patterns) {
        PatternConstraint patternConstraint = new PatternConstraint(target, patterns);
        return patternConstraint;
    }

    public static PatternConstraint buildAndAddToCollection(final SingleTargetReference target,
                                                            ConstraintCollection constraintCollection,
                                                            HashMap<String, Integer> patterns) {
        PatternConstraint patternConstraint = new PatternConstraint(target, patterns);
        constraintCollection.add(patternConstraint);
        return patternConstraint;
    }

    public PatternConstraint(final SingleTargetReference target, HashMap<String, Integer> patterns) {
        Validate.isTrue(target.getAllTargetIds().size() == 1);

        this.patterns = patterns;
        this.target = target;
    }

    @Override
    public String toString() {
        return "PatternConstraint [pattern=" + patterns + "]";
    }

    @Override
    public TargetReference getTargetReference() {
        return target;
    }

    public HashMap<String, Integer> getPatterns() {
        return patterns;
    }

}
