package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiable;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

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
