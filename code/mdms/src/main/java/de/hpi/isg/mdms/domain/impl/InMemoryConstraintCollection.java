package de.hpi.isg.mdms.domain.impl;

import de.hpi.isg.mdms.domain.Constraint;
import de.hpi.isg.mdms.domain.ConstraintCollection;
import de.hpi.isg.mdms.domain.MetadataStore;
import de.hpi.isg.mdms.domain.Target;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * This constraint collection only saves constraints in-memory. It is suitable for dry-runs.
 *
 * @author sebastian.kruse
 * @since 20.02.2015
 */
public class InMemoryConstraintCollection implements ConstraintCollection {

    private final Collection<Target> scope;

    private final MetadataStore metadataStore;

    public InMemoryConstraintCollection(MetadataStore metadataStore, Target scope) {
        this(metadataStore, Collections.singleton(scope));
    }
    public InMemoryConstraintCollection(MetadataStore metadataStore, Collection<Target> scope) {
        this.metadataStore = metadataStore;
        this.scope = scope;
    }

    private final Collection<Constraint> constraints = new LinkedList<>();
    private String description = "in-memory metadata store";

    @Override
    public Collection<Constraint> getConstraints() {
        return this.constraints;
    }

    @Override
    public Collection<Target> getScope() {
        return this.scope;
    }

    @Override
    public void add(Constraint constraint) {
        this.constraints.add(constraint);
    }

    @Override
    public MetadataStore getMetadataStore() {
        return this.metadataStore;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public int getId() {
        return 0;
    }
}
