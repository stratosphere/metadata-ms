package de.hpi.isg.mdms.model.constraints;

import de.hpi.isg.mdms.model.DefaultMetadataStore;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.exceptions.NotAllTargetsInStoreException;

/**
 * The default {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection} implementation that is used by the in-memory {@link de.hpi.isg.mdms.model.DefaultMetadataStore}.
 * 
 * @author fabian
 *
 */

public class DefaultConstraintCollection<T> extends AbstractIdentifiable implements ConstraintCollection<T> {

    private static final long serialVersionUID = -6633086023388829925L;
    private final Set<Constraint> constraints;
    private final Set<Target> scope;

    private String description;
    
    private final Experiment experiment;

    private final Class<T> constrainttype;


    @ExcludeHashCodeEquals
    private final DefaultMetadataStore metadataStore;

    public DefaultConstraintCollection(DefaultMetadataStore metadataStore, int id, Set<Constraint> constraints,
            Set<Target> scope, Experiment experiment, Class<T>  constrainttype) {
        super(id);
        this.metadataStore = metadataStore;
        this.constraints = constraints;
        this.scope = scope;
        this.experiment = experiment;
        this.constrainttype = constrainttype;

    }

    public DefaultConstraintCollection(DefaultMetadataStore metadataStore, int id, Set<Constraint> constraints,
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
        for (IntIterator i = constraint.getTargetReference().getAllTargetIds().iterator(); i.hasNext();) {
            int targetId = i.nextInt();
            if (!this.metadataStore.hasTargetWithId(targetId)) {
                throw new NotAllTargetsInStoreException(targetId);
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

    public Class<T> getConstraintClass(){
        return this.constrainttype;
    }
}
