package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntOpenHashBigSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

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

    transient private File storeLocation;

    private final IntSet idsInUse = new IntOpenHashBigSet();

    @ExcludeHashCodeEquals
    private final IdUtils idUtils;

    @ExcludeHashCodeEquals
    private final Random randomGenerator = new Random();

    public DefaultMetadataStore() {
        this(null, 12, 12);
    }

    public DefaultMetadataStore(File location, int numTableBitsInIds, int numColumnBitsInIds) {
        this.storeLocation = location;

        this.schemas = Collections.synchronizedSet(new HashSet<Schema>());
        this.constraints = Collections.synchronizedSet(new HashSet<Constraint>());
        this.constraintCollections = Collections.synchronizedSet(new HashSet<ConstraintCollection>());
        this.allTargets = Collections.synchronizedSet(new HashSet<Target>());
        this.idUtils = new IdUtils(numTableBitsInIds, numColumnBitsInIds);
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
    public Schema getSchemaByName(final String schemaName) throws NameAmbigousException {
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
        for (int baseSchemaNumber = this.idUtils.getMinSchemaNumber(); baseSchemaNumber <= this.idUtils
                .getMaxSchemaNumber(); baseSchemaNumber++) {
            int schemaNumber = baseSchemaNumber + searchOffset;
            schemaNumber = schemaNumber > this.idUtils.getMaxSchemaNumber() ? schemaNumber
                    - (this.idUtils.getMaxSchemaNumber() - this.idUtils.getMinSchemaNumber()) : schemaNumber;
            final int id = this.idUtils.createGlobalId(schemaNumber);
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
        final int schemaNumber = this.idUtils.getLocalSchemaId(schema.getId());
        final int searchOffset = schema.getTables().size();
        for (int baseTableNumber = this.idUtils.getMinTableNumber(); baseTableNumber <= this.idUtils
                .getMaxTableNumber(); baseTableNumber++) {
            int tableNumber = baseTableNumber + searchOffset;
            tableNumber = tableNumber > this.idUtils.getMaxTableNumber() ? tableNumber
                    - (this.idUtils.getMaxTableNumber() - this.idUtils.getMinTableNumber()) : tableNumber;
            final int id = this.idUtils.createGlobalId(schemaNumber, tableNumber);
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

    @Override
    public IdUtils getIdUtils() {
        return this.idUtils;
    }

    @Override
    public void save(String path) throws IOException {
        File file = new File(path);
        this.storeLocation = file;
        saveToDefaultLocation();
    }

    private void saveToDefaultLocation() throws FileNotFoundException, IOException {
        this.storeLocation.getParentFile().mkdirs();
        final FileOutputStream fout = new FileOutputStream(this.storeLocation);
        final ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(this);
        oos.close();
    }

    @Override
    public void flush() throws Exception {
        if (this.storeLocation == null) {
            Logger.getAnonymousLogger().warning(
                    "Cannot flush metadata store because it has no default saving location.");
        } else {
            saveToDefaultLocation();
        }
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not supported yet.");
        // return null;
    }

    @Override
    public Schema getSchemaById(int schemaId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not supported yet.");
        // return null;
    }

}
