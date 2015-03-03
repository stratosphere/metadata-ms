package de.hpi.isg.mdms.domain;

import java.util.Collection;

import de.hpi.isg.mdms.domain.common.Identifiable;
import de.hpi.isg.mdms.domain.targets.Schema;

/**
 * A {@link ConstraintCollection} groups several {@link Constraint}s that were collected by an experiment on a given
 * scope.
 * 
 * @author fabian
 *
 */

public interface ConstraintCollection extends Identifiable, Described {

    /**
     * This function returns all {@link Constraint}s of this collection.
     * 
     * @return {@link Collection} of all containing {@link Constraint}s.
     */
    public Collection<Constraint> getConstraints();

    /**
     * This functions returns the scope of this {@link ConstraintCollection}. The scope is a {@link Collection} of
     * {@link Target}. If the parent {@link Schema} of one of this scope targets (or if a scope element is a
     * {@link Schema} itself) is deleted, this {@link ConstraintCollection} will be deleted too.
     * 
     * @return
     */
    public Collection<Target> getScope();

    /**
     * Adds a new {@link Constraint} to this collection.
     * 
     * @param constraint
     *        The {@link Constraint} to add.
     */
    public void add(Constraint constraint);

    /**
     * This function returns the {@link MetadataStore} this collection belongs to.
     * 
     * @return the {@link MetadataStore}.
     */
    public MetadataStore getMetadataStore();
}
