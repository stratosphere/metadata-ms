package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntOpenHashBigSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.exceptions.IdAlreadyInUseException;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

/**
 * The default implementation of the {@link MetadataStore}.
 *
 */

public class DefaultMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private final Collection<Schema> schemas;

    private final Collection<ConstraintCollection> constraintCollections;

    private final Collection<Constraint> constraints;

    private final Collection<Target> allTargets;

    private final IntSet idsInUse = new IntOpenHashBigSet();

    @ExcludeHashCodeEquals
    private final Random randomGenerator = new Random();

    public DefaultMetadataStore() {
        this.schemas = Collections.synchronizedSet(new HashSet<Schema>());
        this.constraints = Collections.synchronizedSet(new HashSet<Constraint>());
        this.constraintCollections = Collections.synchronizedSet(new HashSet<ConstraintCollection>());
        this.allTargets = Collections.synchronizedSet(new HashSet<Target>());
    }

    @Override
    public void addConstraint(final Constraint constraint) {
        for (final Target target : constraint.getTargetReference().getAllTargets()) {
            if (!this.allTargets.contains(target)) {
                throw new NotAllTargetsInStoreException(target);
            }

        }
        this.constraints.add(constraint);
    }

    @Override
    public void addSchema(final Schema schema) {
        this.schemas.add(schema);
    }

    @Override
    public Schema addSchema(final String name, final Location location) {
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
    public Schema getSchema(final String schemaName) throws NameAmbigousException {
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
            final int id = IdUtils.createGlobalId(schemaNumber);
            if (!this.idIsInUse(id)) {
                return id;
            }
        }
        throw new IllegalStateException(String.format("No free schema ID left within schema."));
    }

    @Override
    public int getUnusedConstraintCollectonId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    @Override
    public int getUnusedTableId(final Schema schema) {
        Validate.isTrue(this.schemas.contains(schema));
        final int schemaNumber = IdUtils.getLocalSchemaId(schema.getId());
        final int searchOffset = schema.getTables().size();
        for (int baseTableNumber = IdUtils.MIN_TABLE_NUMBER; baseTableNumber <= IdUtils.MAX_TABLE_NUMBER; baseTableNumber++) {
            int tableNumber = baseTableNumber + searchOffset;
            tableNumber = tableNumber > IdUtils.MAX_TABLE_NUMBER ? tableNumber
                    - (IdUtils.MAX_TABLE_NUMBER - IdUtils.MIN_TABLE_NUMBER) : tableNumber;
            final int id = IdUtils.createGlobalId(schemaNumber, tableNumber);
            if (!this.idIsInUse(id)) {
                return id;
            }
        }
        throw new IllegalStateException(String.format("No free table ID left within schema %s.", schema));
    }

    private boolean idIsInUse(final int id) {
        synchronized (this.idsInUse) {
            return this.idsInUse.contains(id);
        }
    }

    @Override
    public void registerId(final int id) {
        synchronized (this.idsInUse) {
            if (!this.idsInUse.add(id)) {
                throw new IdAlreadyInUseException("id is already in use: " + id);
            }
        }
    }

    @Override
    public void registerTargetObject(final Target message) {
        this.allTargets.add(message);
    }

    @Override
    public String toString() {
        return "MetadataStore[" + this.schemas.size() + " schemas, " + this.constraints.size() + " constraints]";
    }

    @Override
    public Collection<ConstraintCollection> getConstraintCollections() {
        return this.constraintCollections;
    }

    @Override
    public void addConstraintCollection(ConstraintCollection constraintCollection) {
        for (Constraint constr : constraintCollection.getConstraints()) {
            this.addConstraint(constr);
        }
        this.constraintCollections.add(constraintCollection);
    }

    @Override
    public ConstraintCollection createConstraintCollection() {
        ConstraintCollection constraintCollection = new DefaultConstraintCollection(getUnusedConstraintCollectonId(),
                new HashSet<Constraint>(), new HashSet<Target>());
        this.constraintCollections.add(constraintCollection);
        return constraintCollection;
    }
}
