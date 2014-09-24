package de.hpi.isg.metadata_store.domain.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

/**
 * The default implementation of the {@link MetadataStore}.
 *
 */

public class DefaultMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private final Collection<Schema> schemas;

    private final Collection<Constraint> constraints;

    private final Collection<Target> allTargets;

    public DefaultMetadataStore() {
	this.schemas = new HashSet<Schema>();
	this.constraints = new HashSet<Constraint>();
	this.allTargets = new HashSet<>();
    }

    @Override
    public void addConstraint(Constraint constraint) {
	for (final Target target : constraint.getTargetReference().getAllTargets()) {
	    if (!this.allTargets.contains(target)) {
		throw new NotAllTargetsInStoreException(target);
	    }

	}

    }

    @Override
    public void addSchema(Schema schema) {
	this.schemas.add(schema);

    }

    @Override
    public Collection<Target> getAllTargets() {
	return Collections.unmodifiableCollection(this.allTargets);
    }

    @Override
    public Collection<Constraint> getConstraints() {
	return Collections.unmodifiableCollection(this.constraints);
    }

    @Override
    public Collection<Schema> getSchemas() {
	return this.schemas;
    }

    @Override
    public String toString() {
	return "MetadataStore [schemas=" + this.schemas + ", constraints=" + this.constraints + ", allTargets="
		+ this.allTargets + ", getSchemas()=" + this.getSchemas() + ", getConstraints()="
		+ this.getConstraints() + "]";
    }

    @Override
    public void update(Object message) {
	if (message instanceof Target) {
	    this.allTargets.add((Target) message);
	}
    }
}
