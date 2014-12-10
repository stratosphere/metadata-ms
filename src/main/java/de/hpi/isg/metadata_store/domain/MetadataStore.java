package de.hpi.isg.metadata_store.domain;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * A {@link MetadataStore} stores schema information as well as {@link Constraint}s holding on the objects stored in it.
 *
 */

public interface MetadataStore extends Serializable, Observer {

    /**
     * This method adds the given {@link ConstraintCollection} to the {@link MetadataStore}s collection of known
     * {@link ConstraintCollection}s. It also adds all the constraints given in the {@link ConstraintCollection} for
     * consistence.
     * 
     * @param constraintCollection
     */
    public void addConstraintCollection(ConstraintCollection constraintCollection);

    @Deprecated
    public void addConstraint(Constraint constraint);

    /**
     * @deprecated use {@link #addSchema(String, Location)} instead
     */
    @Deprecated
    public void addSchema(Schema schema);

    public Schema addSchema(String name, Location location);

    public Collection<Target> getAllTargets();

    public Collection<ConstraintCollection> getConstraintCollections();

    public Collection<Constraint> getConstraints();

    /**
     * Retrieve a schema from the store if it exists, throws {@link NameAmbigousException} if there are more than one
     * with that name
     *
     * @param schemaName
     * @return
     */
    public Schema getSchemaByName(String schemaName) throws NameAmbigousException;

    /**
     * Retrieve a {@link Collection} of schemas from the store for the given name
     *
     * @param schemaName
     * @return
     */
    public Collection<Schema> getSchemasByName(String schemaName);

    /**
     * Retrieve a schema from the store if it exists for the given id
     *
     * @param schemaId
     * @return
     */
    public Schema getSchemaById(int schemaId);

    /**
     * Get all knwon {@link Schema}s.
     * 
     * @return
     */
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

    int getUnusedConstraintCollectonId();

    ConstraintCollection createConstraintCollection(Target... scope);

    IdUtils getIdUtils();

    /**
     * Saves this store to the given path, if manual saving is supported.
     * 
     * @throws IOException
     */
    public void save(String path) throws IOException;
    
    /**
     * Saves any pending changes in the metadata store.
     * @throws Exception if the saving fails
     */
    public void flush() throws Exception;

}
