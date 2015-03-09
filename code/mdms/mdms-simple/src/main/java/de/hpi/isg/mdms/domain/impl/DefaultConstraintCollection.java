package de.hpi.isg.mdms.domain.impl;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import de.hpi.isg.mdms.domain.Constraint;
import de.hpi.isg.mdms.domain.ConstraintCollection;
import de.hpi.isg.mdms.domain.Target;
import de.hpi.isg.mdms.domain.common.impl.AbstractIdentifiable;
import de.hpi.isg.mdms.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.exceptions.NotAllTargetsInStoreException;

/**
 * The default {@link de.hpi.isg.mdms.domain.ConstraintCollection} implementation that is used by the in-memory {@link de.hpi.isg.mdms.domain.impl.DefaultMetadataStore}.
 * 
 * @author fabian
 *
 */

public class DefaultConstraintCollection extends AbstractIdentifiable implements ConstraintCollection {

    private static final long serialVersionUID = -6633086023388829925L;
    private final Set<Constraint> constraints;
    private final Set<Target> scope;

    private String description;

    @ExcludeHashCodeEquals
    private final DefaultMetadataStore metadataStore;

    public DefaultConstraintCollection(DefaultMetadataStore metadataStore, int id, Set<Constraint> constraints,
            Set<Target> scope) {
        super(id);
        this.metadataStore = metadataStore;
        this.constraints = constraints;
        this.scope = scope;
    }

    @Override
    public Collection<Constraint> getConstraints() {
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
    public void add(Constraint constraint) {
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

}
