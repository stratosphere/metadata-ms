package de.hpi.isg.metadata_store.domain;

import java.util.Collection;

/**
 * A {@link ConstraintCollection} groups several {@link Constraint}s that were collected by an experiment on a given
 * scope.
 * 
 * @author fabian
 *
 */

public interface ConstraintCollection {
    public Collection<Constraint> getConstraints();

    public Collection<Target> getScope();
}
