package de.hpi.isg.mdms.domain;

import java.io.Serializable;
import java.util.Map;

import de.hpi.isg.mdms.domain.constraints.impl.ConstraintSQLSerializer;
import de.hpi.isg.mdms.domain.factories.SQLInterface;
import de.hpi.isg.mdms.domain.targets.Column;

/**
 * This interface is the super type for all kinds of constraints that can hold on data represented in the
 * {@link MetadataStore}. All constraints embody a {@link Map} with keys and values. Further they reference
 * {@link Target}s via {@link TargetReference}s. E.g. a constraint can hold on a single {@link Column}, its data type
 * for example. Further constraints can have multiple {@link Target}s in their {@link TargetReference} like an IND, FD,
 * etc. Sub types may store additional information outside of the included {@link Map} fore easier use.
 *
 */
public interface Constraint extends Serializable {
    public enum BASIC_STATS {
        TYPE
    }

    public enum STRING_STATS {
        MIN_STRING, MAX_STRING, MIN_LENGTH, MAX_LENGTH, AVG_LENGTH, DISTINCT_COUNT
    };

    public enum INTEGER_STATS {
        MIN_INTEGER, MAX_INTEGER, AVG_INTEGER
    };

    public enum DECIMAL_STATS {
        MIN_DECIMAL, MAX_DECIMAL, AVG_DECIMAL
    };

    /**
     * This functions returns the corresponding {@link ConstraintCollection}.
     * 
     * @return the {@link ConstraintCollection}
     */
    public ConstraintCollection getConstraintCollection();

    /**
     * This function returns the {@link TargetReference} including all {@link Target}(s) of this constraint.
     * 
     * @return the {@link TargetReference}
     */
    public TargetReference getTargetReference();

    /**
     * This function returns a new copy of the constraint's own {@link ConstraintSQLSerializer}. Therefore you have to
     * pass a {@link SQLInterface}.
     * 
     * @param sqlInterface
     *        {@link SQLInterface}
     * @return a new copy of a {@link ConstraintSQLSerializer}
     */
    public ConstraintSQLSerializer<? extends Constraint> getConstraintSQLSerializer(SQLInterface sqlInterface);

}
