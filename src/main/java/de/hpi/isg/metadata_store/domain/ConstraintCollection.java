package de.hpi.isg.metadata_store.domain;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Identifiable;

/**
 * A {@link ConstraintCollection} groups several {@link Constraint}s that were collected by an experiment on a given
 * scope.
 * 
 * @author fabian
 *
 */

public interface ConstraintCollection extends Identifiable, Described {

    public Collection<Constraint> getConstraints();

    public Collection<Target> getScope();

    public void add(Constraint constraint);
    
    public MetadataStore getMetadataStore();
}
