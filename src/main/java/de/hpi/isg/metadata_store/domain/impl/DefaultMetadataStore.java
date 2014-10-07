package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.exceptions.IdAlreadyInUseException;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * The default implementation of the {@link MetadataStore}.
 *
 */

public class DefaultMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private final Collection<Schema> schemas;

    private final Collection<Constraint> constraints;

    private final Collection<Target> allTargets;

    private final IntSet idsInUse = new IntOpenHashSet();

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
	this.constraints.add(constraint);
    }

    @Override
    public void addSchema(Schema schema) {
	this.schemas.add(schema);
    }

    @Override
    public Schema addSchema(String name, Location location) {
	final int id = this.getUnusedSchemaId();
	final Schema schema = DefaultSchema.buildAndRegister(this, id, name, location);
	this.addSchema(schema);
	return schema;
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
    public Schema getSchema(String schemaName) throws NameAmbigousException {
	final List<Schema> results = new ArrayList<>();
	for (final Schema schema : this.schemas) {
	    if (schema.getName().equals(schemaName)) {
		results.add(schema);
	    }
	}
	if (results.size() > 1) {
	    throw new NameAmbigousException(schemaName);
	}
	if (results.isEmpty()) {
	    return null;
	}
	return results.get(0);
    }

    @Override
    public Collection<Schema> getSchemas() {
	return this.schemas;
    }

    @Override
    public int getUnusedSchemaId() {
	final int searchOffset = this.getSchemas().size();
	for (int baseSchemaNumber = IdUtils.MIN_SCHEMA_NUMBER; baseSchemaNumber <= IdUtils.MAX_SCHEMA_NUMBER; baseSchemaNumber++) {
	    int schemaNumber = baseSchemaNumber + searchOffset;
	    schemaNumber = schemaNumber > IdUtils.MAX_SCHEMA_NUMBER ? schemaNumber
		    - (IdUtils.MAX_SCHEMA_NUMBER - IdUtils.MIN_SCHEMA_NUMBER) : schemaNumber;
		    final int id = IdUtils.createGlobalId(schemaNumber, baseSchemaNumber);
		    if (!this.idIsInUse(id)) {
			return id;
		    }
	}
	throw new IllegalStateException(String.format("No free schema ID left within schema."));
    }

    @Override
    public int getUnusedTableId(Schema schema) {
	Validate.isTrue(this.schemas.contains(schema));
	final int schemaNumber = IdUtils.getLocalSchemaId(schema.getId());
	final int searchOffset = schema.getTables().size();
	for (int baseTableNumber = IdUtils.MIN_TABLE_NUMBER; baseTableNumber <= IdUtils.MAX_TABLE_NUMBER; baseTableNumber++) {
	    int tableNumber = baseTableNumber + searchOffset;
	    tableNumber = tableNumber > IdUtils.MAX_TABLE_NUMBER ? tableNumber
		    - (IdUtils.MAX_TABLE_NUMBER - IdUtils.MIN_TABLE_NUMBER) : tableNumber;
		    final int id = IdUtils.createGlobalId(schemaNumber, baseTableNumber);
		    if (!this.idIsInUse(id)) {
			return id;
		    }
	}
	throw new IllegalStateException(String.format("No free table ID left within schema %s.", schema));
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
	return "MetadataStore[" + this.schemas.size() + " schemas, " + this.constraints.size() + " constraints]";
    }
}
