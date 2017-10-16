package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Target;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * This constraint collection only saves constraints in-memory. It is suitable for dry-runs.
 *
 * @author sebastian.kruse
 * @since 20.02.2015
 */
public class InMemoryConstraintCollection<T> implements ConstraintCollection<T> {

    private final String userDefinedId;

    private final Collection<Target> scope;

    private final Experiment experiment;

    private final Class<T> constrainttype;

    private final MetadataStore metadataStore;


    public InMemoryConstraintCollection(MetadataStore metadataStore, String userDefinedId, Target scope, Class<T> constrainttype) {
        this(metadataStore, userDefinedId, Collections.singleton(scope), constrainttype);
    }

    public InMemoryConstraintCollection(MetadataStore metadataStore, String userDefinedId, Collection<Target> scope, Class<T> constrainttype) {
        this.metadataStore = metadataStore;
        this.userDefinedId = userDefinedId;
        this.scope = scope;
        this.experiment = null;
        this.constrainttype = constrainttype;
    }

    public InMemoryConstraintCollection(MetadataStore metadataStore, String userDefinedId, Experiment experiment, Collection<Target> scope, Class<T> constrainttype) {
        this.metadataStore = metadataStore;
        this.userDefinedId = userDefinedId;
        this.scope = scope;
        this.experiment = experiment;
        this.constrainttype = constrainttype;
    }

    private final Collection<T> constraints = new LinkedList<>();
    private String description = "in-memory metadata store";

    @Override
    public Collection<T> getConstraints() {
        return this.constraints;
    }

    @Override
    public Collection<Target> getScope() {
        return this.scope;
    }

    @Override
    public void add(T constraint) {
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

    @Override
    public String getUserDefinedId() {
        return this.userDefinedId;
    }

    @Override
    public Experiment getExperiment() {
        return this.experiment;
    }

    @Override
    public Class<T> getConstraintClass() {
        return this.constrainttype;
    }
}
