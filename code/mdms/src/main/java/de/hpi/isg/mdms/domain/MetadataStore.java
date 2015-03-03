package de.hpi.isg.mdms.domain;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.mdms.domain.common.Observer;
import de.hpi.isg.mdms.domain.targets.Schema;
import de.hpi.isg.mdms.domain.util.IdUtils;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;

/**
 * A {@link MetadataStore} stores schema information as well as {@link Constraint}s holding on the objects stored in it.
 *
 */

public interface MetadataStore extends Serializable, Observer {

    /**
     * Creates and adds a new {@link Schema} to this store.
     * 
     * @param name
     * @param description
     * @param location
     * @return the newly created {@link Schema}
     */
    public Schema addSchema(String name, String description, Location location);

    /**
     * Checks whether this store includes a {@link Target} with that id.
     * 
     * @param id
     * @return true if target with id is contained, else false.
     */
    public boolean hasTargetWithId(int id);

    /**
     * Returns a {@link Collection} of all {@link ConstraintCollection}s.
     * 
     * @return all {@link ConstraintCollection}s.
     */
    public Collection<ConstraintCollection> getConstraintCollections();

    /**
     * Returns a particular {@link ConstraintCollection} with the given id.
     * 
     * @param id
     * @return the {@link ConstraintCollection} with the given id, <code>null</code> if no exists with given id.
     */
    public ConstraintCollection getConstraintCollection(int id);

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

    /**
     * Returns an unused id for a {@link ConstraintCollection}.
     * 
     * @return unused {@link ConstraintCollection} id.
     */
    int getUnusedConstraintCollectonId();

    /**
     * This method creates a new {@link ConstraintCollection} that will also be added to this {@link MetadataStore}s
     * collection of known {@link ConstraintCollection}s.
     */
    ConstraintCollection createConstraintCollection(String description, Target... scope);

    /**
     * Returns the {@link IdUtils} of this store.
     * 
     * @return the {@link IdUtils}
     */
    IdUtils getIdUtils();

    /**
     * Saves this store to the given path, if manual saving is supported.
     * 
     * @throws IOException
     * @deprecated Not all MetadataStores support saving to a path, so rather use {@link #flush()}.
     */
    public void save(String path) throws IOException;

    /**
     * Saves any pending changes in the metadata store.
     * 
     * @throws Exception
     *         if the saving fails
     */
    public void flush() throws Exception;

    /**
     * Removes a {@link Schema} and ALL child {@link Target} objects from the store. Also ALL
     * {@link ConstraintCollection} and containing {@link Constraint}s will be deleted that have the {@link Schema} or a
     * child element in it's scope.
     * 
     * @param schema
     *        the {@link Schema} to remove
     */
    public void removeSchema(Schema schema);

    /**
     * Removes the {@link ConstraintCollection} from the store as well as all {@link Constraint}.
     * 
     * @param constraintCollection
     */
    public void removeConstraintCollection(ConstraintCollection constraintCollection);

}
