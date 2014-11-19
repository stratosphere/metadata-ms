package de.hpi.isg.metadata_store.domain.impl;

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
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.exceptions.IdAlreadyInUseException;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

/**
 * The default implementation of the {@link MetadataStore}.
 *
 */

public class RDBMSMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final long serialVersionUID = 400271996998552017L;

    @ExcludeHashCodeEquals
    transient final SQLInterface sqlInterface;

    @ExcludeHashCodeEquals
    transient final Random randomGenerator = new Random();
    
    @ExcludeHashCodeEquals
    transient final IdUtils idUtils;

    public RDBMSMetadataStore(SQLiteInterface sqliteInterface) {
        this(sqliteInterface, 12, 12);
    }
    
    public RDBMSMetadataStore(SQLiteInterface sqliteInterface, int numTableBitsInIds, int numColumnBitsInIds) {
        this.sqlInterface = sqliteInterface;
        this.sqlInterface.setMetadataStore(this);
        this.idUtils = new IdUtils(numTableBitsInIds, numColumnBitsInIds);
    }

    @Override
    public void addConstraint(final Constraint constraint) {
        for (final Target target : constraint.getTargetReference().getAllTargets()) {
            if (!this.sqlInterface.getAllTargets().contains(target)) {
                throw new NotAllTargetsInStoreException(target);
            }

        }
        this.sqlInterface.addConstraint(constraint);
    }

    @Override
    public void addSchema(final Schema schema) {
        this.sqlInterface.addSchema(schema);
    }

    @Override
    public Schema addSchema(final String name, final Location location) {
        final int id = this.getUnusedSchemaId();
        final Schema schema = RDBMSSchema.buildAndRegisterAndAdd(this, id, name, location);
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
        return Collections.unmodifiableCollection(this.sqlInterface.getAllTargets());
    }

    @Override
    public Collection<Constraint> getConstraints() {
        return Collections.unmodifiableCollection(this.sqlInterface.getAllConstraintsOrOfConstraintCollection(null));
    }

    @Override
    public Schema getSchema(final String schemaName) throws NameAmbigousException {
        final List<Schema> results = new ArrayList<>();
        for (final Schema schema : this.sqlInterface.getAllSchemas()) {
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
        return Collections.unmodifiableCollection(this.sqlInterface.getAllSchemas());
    }

    @Override
    public int getUnusedSchemaId() {
        final int searchOffset = this.getSchemas().size();
        for (int baseSchemaNumber = this.idUtils.getMinSchemaNumber(); baseSchemaNumber <= this.idUtils.getMaxSchemaNumber(); baseSchemaNumber++) {
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
    public int getUnusedTableId(final Schema schema) {
        Validate.isTrue(this.sqlInterface.getAllSchemas().contains(schema));
        final int schemaNumber = this.idUtils.getLocalSchemaId(schema.getId());
        final int searchOffset = schema.getTables().size();
        for (int baseTableNumber = this.idUtils.getMinTableNumber(); baseTableNumber <= this.idUtils.getMaxTableNumber(); baseTableNumber++) {
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
        synchronized (this.sqlInterface.getIdsInUse()) {

            return this.sqlInterface.getIdsInUse().contains(id);
        }
    }

    @Override
    public void registerId(final int id) {
        synchronized (this.sqlInterface.getIdsInUse()) {
            if (!this.sqlInterface.addToIdsInUse(id)) {
                throw new IdAlreadyInUseException("id is already in use: " + id);
            }
        }
    }

    @Override
    public void registerTargetObject(final Target target) {
        this.sqlInterface.addTarget(target);
    }

    @Override
    public String toString() {
        return "MetadataStore[" + this.sqlInterface.getAllSchemas().size() + " schemas, "
                + this.sqlInterface.getAllConstraintsOrOfConstraintCollection(null) + " constraints]";
    }

    @Override
    public Collection<ConstraintCollection> getConstraintCollections() {
        return this.sqlInterface.getAllConstraintCollections();
    }

    @Override
    public void addConstraintCollection(ConstraintCollection constraintCollection) {

        this.sqlInterface.addConstraintCollection(constraintCollection);

        for (Constraint constr : constraintCollection.getConstraints()) {
            this.addConstraint(constr);
        }

        for (Target target : constraintCollection.getScope()) {
            this.sqlInterface.addScope(target, constraintCollection);
        }

    }

    public SQLInterface getSQLInterface() {
        return this.sqlInterface;
    }

    @Override
    public int getUnusedConstraintCollectonId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    @Override
    public ConstraintCollection createConstraintCollection() {
        ConstraintCollection constraintCollection = new RDBMSConstraintCollection(getUnusedConstraintCollectonId(),
                new HashSet<Constraint>(), new HashSet<Target>(), getSQLInterface());
        return constraintCollection;
    }
    
    /**
     * @return the idUtils
     */
    @Override
    public IdUtils getIdUtils() {
        return idUtils;
    }
}
