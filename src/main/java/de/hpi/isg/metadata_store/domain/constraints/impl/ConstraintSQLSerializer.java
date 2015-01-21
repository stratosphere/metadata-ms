package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.util.Collection;
import java.util.List;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;

public interface ConstraintSQLSerializer {

    void serialize(Integer constraintId, Constraint constraint);

    Collection<Constraint> deserializeConstraintsForConstraintCollection(ConstraintCollection constraintCollection);

    List<String> getTableNames();

    void initializeTables();

    void removeConstraintsForConstraintCollection(ConstraintCollection constraintCollection);

}
