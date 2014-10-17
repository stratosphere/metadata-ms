package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * A {@link MetadataStore} stores schema information as well as {@link Constraint}s holding on the objects stored in it.
 *
 */

public interface MetadataStore extends Serializable, Observer {
    public void addConstraint(Constraint constraint);

    /**
     * @deprecated use {@link #addSchema(String, Location)} instead
     */
    @Deprecated
    public void addSchema(Schema schema);

    public Schema addSchema(String name, Location location);

    public Collection<Target> getAllTargets();

    public Collection<Constraint> getConstraints();

    /**
     * Retrieve a schema from the store if it exists
     *
     * @param schemaName
     * @return
     */
    public Schema getSchema(String schemaName) throws NameAmbigousException;

    public Collection<Schema> getSchemas();

    /**
     * Looks for an ID that can be assigned to a new schema.
     *
     * @return the unused schema ID
     */
    int getUnusedSchemaId();

    /**
     * Looks for an ID that can be assigned to a new table within the given schema.
     *
     * @param schema
     *        is the schema to which the new table shall be added
     * @return the unused table ID
     */
    int getUnusedTableId(Schema schema);
}
