package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Map;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.constraints.impl.ConstraintSQLSerializer;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * This interface is the super type for all kinds of constraints that can hold on data represented in the
 * {@link MetadataStore}. All constraints embody a {@link Map} with keys and values. Further they reference
 * {@link Target}s via {@link TargetReference}s. E.g. a constraint can hold on a single {@link Column}, its data type
 * for example. Further constraints can have multiple {@link Target}s in their {@link TargetReference} like an IND, FD,
 * etc. Sub types may store additional information outside of the included {@link Map} fore easier use.
 *
 */
public interface Constraint extends Serializable, Identifiable {
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

    public ConstraintCollection getConstraintCollection();

    public TargetReference getTargetReference();

    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface);

}
