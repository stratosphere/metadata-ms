package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.exceptions.IdAlreadyInUseException;
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

    private final IntSet idsInUse = new IntArraySet();

    @ExcludeHashCodeEquals
    private final Random randomGenerator = new Random();

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
    public int generateRandomId() {
	final int id = Math.abs(this.randomGenerator.nextInt(Integer.MAX_VALUE));
	if (this.idIsInUse(id)) {
	    return this.generateRandomId();
	}
	return id;
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

    private boolean idIsInUse(int id) {
	return this.idsInUse.contains(id);
    }

    @Override
    public void registerId(int id) {
	if (this.idsInUse.contains(id)) {
	    throw new IdAlreadyInUseException("id is already in use: " + id);
	}
	this.idsInUse.add(id);
    }

    @Override
    public void registerTargetObject(Target message) {
	this.allTargets.add(message);
    }

    @Override
    public String toString() {
	return "MetadataStore [schemas=" + this.schemas + ", constraints=" + this.constraints + ", allTargets="
		+ this.allTargets + ", getSchemas()=" + this.getSchemas() + ", getConstraints()="
		+ this.getConstraints() + "]";
    }
}
