package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;

public interface ConstraintSQLSerializer {

    void serialize(Integer constraintId, Constraint constraint);

    Collection<Constraint> deserializeConstraintsForConstraintCollection(ConstraintCollection constraintCollection);

}
