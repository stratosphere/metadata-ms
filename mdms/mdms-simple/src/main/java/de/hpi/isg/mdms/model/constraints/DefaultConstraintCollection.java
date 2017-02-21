package de.hpi.isg.mdms.model.constraints;

import de.hpi.isg.mdms.exceptions.NotAllTargetsInStoreException;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Target;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * The default {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection} implementation that is used by the in-memory {@link de.hpi.isg.mdms.model.DefaultMetadataStore}.
 *
 * @author fabian
 */

public class DefaultConstraintCollection<T extends Constraint> extends AbstractIdentifiable implements ConstraintCollection<T> {

    private static final long serialVersionUID = -6633086023388829925L;
    private final Set<T> constraints;
    private final Set<Target> scope;

    private String description;

    private final Experiment experiment;

    private final Class<T> constrainttype;


    @ExcludeHashCodeEquals
    private final DefaultMetadataStore metadataStore;

    public DefaultConstraintCollection(DefaultMetadataStore metadataStore, int id, Set<T> constraints,
                                       Set<Target> scope, Experiment experiment, Class<T> constrainttype) {
        super(id);
        this.metadataStore = metadataStore;
        this.constraints = constraints;
        this.scope = scope;
        this.experiment = experiment;
        this.constrainttype = constrainttype;

    }

    public DefaultConstraintCollection(DefaultMetadataStore metadataStore, int id, Set<T> constraints,
                                       Set<Target> scope, Class<T> constrainttype) {
        super(id);
        this.metadataStore = metadataStore;
        this.constraints = constraints;
        this.scope = scope;
        this.experiment = null;
        this.constrainttype = constrainttype;
    }

    @Override
    public Collection<T> getConstraints() {
        return Collections.unmodifiableCollection(this.constraints);
    }

    @Override
    public Collection<Target> getScope() {
        return Collections.unmodifiableCollection(this.scope);
    }

    @Override
    public String toString() {
        return "DefaultConstraintCollection [constraints=" + constraints + ", scope=" + scope + "]";
    }

    @Override
    public void add(T constraint) {
        for (int id : constraint.getAllTargetIds()) {
            if (!this.metadataStore.hasTargetWithId(id)) {
                throw new NotAllTargetsInStoreException(id);
            }
        }

        this.constraints.add(constraint);
    }

    @Override
    public DefaultMetadataStore getMetadataStore() {
        return this.metadataStore;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Experiment getExperiment() {
        return this.experiment;
    }

    public Class<T> getConstraintClass() {
        return this.constrainttype;
    }
}
