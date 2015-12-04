package de.hpi.isg.mdms.model.constraints;

import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.model.targets.Column;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface is the super type for all kinds of constraints that can hold on data represented in the
 * {@link de.hpi.isg.mdms.model.MetadataStore}. All constraints embody a {@link Map} with keys and values. Further they reference
 * {@link de.hpi.isg.mdms.model.targets.Target}s via {@link de.hpi.isg.mdms.model.targets.TargetReference}s. E.g. a constraint can hold on a single {@link Column}, its data type
 * for example. Further constraints can have multiple {@link de.hpi.isg.mdms.model.targets.Target}s in their {@link de.hpi.isg.mdms.model.targets.TargetReference} like an IND, FD,
 * etc. Sub types may store additional information outside of the included {@link Map} fore easier use.
 *
 */
public interface Constraint extends Serializable {
    enum BASIC_STATS {
        TYPE
    }

    enum STRING_STATS {
        MIN_STRING, MAX_STRING, MIN_LENGTH, MAX_LENGTH, AVG_LENGTH, DISTINCT_COUNT
    };

    enum INTEGER_STATS {
        MIN_INTEGER, MAX_INTEGER, AVG_INTEGER
    };

    enum DECIMAL_STATS {
        MIN_DECIMAL, MAX_DECIMAL, AVG_DECIMAL
    };

    /**
     * This function returns the {@link de.hpi.isg.mdms.model.targets.TargetReference} including all {@link de.hpi.isg.mdms.model.targets.Target}(s) of this constraint.
     * 
     * @return the {@link de.hpi.isg.mdms.model.targets.TargetReference}
     */
    TargetReference getTargetReference();

}
