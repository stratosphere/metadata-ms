package de.hpi.isg.metadata_store.domain.constraints.impl;

import de.hpi.isg.metadata_store.domain.Constraint;

public interface ConstraintSQLSerializer {

    void serialize(Integer constraintId, Constraint constraint);

}
