package de.hpi.isg.mdms.model.constraints;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.Described;
import de.hpi.isg.mdms.model.common.Identifiable;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Target;

import java.util.Collection;

/**
 * A {@link ConstraintCollection} groups several {@link Constraint}s that were collected by an experiment on a given
 * scope.
 *
 * @author fabian
 * @author Sebastian Kruse
 */
public interface ConstraintCollection<T> extends Identifiable, Described {

    /**
     * This function provides a user-defined ID for this instance.
     *
     * @return the ID or {@code null} if none
     */
    String getUserDefinedId();

    /**
     * This functions returns the corresponding {@link de.hpi.isg.mdms.model.experiment.Experiment}.
     *
     * @return the {@link de.hpi.isg.mdms.model.experiment.Experiment}
     */
    Experiment getExperiment();

    /**
     * This function returns all {@link Constraint}s of this collection.
     *
     * @return {@link Collection} of all containing {@link Constraint}s.
     */
    Collection<T> getConstraints();

    /**
     * This functions returns the scope of this {@link ConstraintCollection}. The scope is a {@link Collection} of
     * {@link de.hpi.isg.mdms.model.targets.Target}. If the parent {@link de.hpi.isg.mdms.model.targets.Schema} of on
     * e of this scope targets (or if a scope element is a
     * {@link de.hpi.isg.mdms.model.targets.Schema} itself) is deleted, this {@link ConstraintCollection} will be deleted too.
     *
     * @return
     */
    Collection<Target> getScope();

    /**
     * Adds a new {@link Constraint} to this collection.
     *
     * @param constraint The {@link Constraint} to add.
     */
    void add(T constraint);

    /**
     * This function returns the {@link de.hpi.isg.mdms.model.MetadataStore} this collection belongs to.
     *
     * @return the {@link de.hpi.isg.mdms.model.MetadataStore}.
     */
    MetadataStore getMetadataStore();

    /**
     * Provides the {@link Class} of the {@link Constraint}s stored in this instance.
     *
     * @return the {@link Class}
     */
    Class<T> getConstraintClass();
}
