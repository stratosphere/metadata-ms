package de.hpi.isg.mdms.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;

/**
 * A {@link MetadataStore} stores schema information as well as {@link de.hpi.isg.mdms.model.constraints.Constraint}s holding on the objects stored in it.
 *
 */

public interface MetadataStore extends Serializable, Observer<Target> {

    /**
     * Creates and adds a new {@link de.hpi.isg.mdms.model.targets.Schema} to this store.
     * 
     * @param name
     * @param description
     * @param location
     * @return the newly created {@link de.hpi.isg.mdms.model.targets.Schema}
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
     * Returns a {@link Collection} of all {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}s.
     * 
     * @return all {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}s.
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
     * Looks for an ID that can be assigned to a new algorithm.
     *
     * @return the unused algorithm ID
     */
    int getUnusedAlgorithmId();

    /**
     * Looks for an ID that can be assigned to a new experiment.
     *
     * @return the unused experiment ID
     */
    int getUnusedExperimentId();

    
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
     * This method creates a new {@link Algorithm} that will also be added to this {@link MetadataStore}s
     * collection of known {@link ConstraintCollection}s.
     */
    Algorithm createAlgorithm(String name);
    
    /**
     * Retrieve an algorithm from the store if it exists for the given id
     *
     * @param algorithmId
     * @return {@link Algorithm}
     */
    public Algorithm getAlgorithmById(int schemaId);

    /**
     * Retrieve an algorithm from the store if it exists for the given name
     *
     * @param algorithmName
     * @return {@link Algorithm}
     */
    public Algorithm getAlgorithmByName(String name);
    
    
    /**
     * Get all knwon {@link Algorithms}s.
     * 
     * @return {@link Collection} of {@link Algorithm}s.
     */
    public Collection<Algorithm> getAlgorithms();

    
    /**
     * Get all knwon {@link Experiment}s.
     * 
     * @return {@link Collection} of {@link Experiment}s.
     */
    public Collection<Experiment> getExperiments();

    
    /**
     * This method creates a new {@link Experiment} that will also be added to this {@link MetadataStore}s
     * collection of known {@link Experiment}s.
     */
    Experiment createExperiment(String description, Algorithm algorithm);
    
    /**
     * This method creates a new {@link ConstraintCollection} that will also be added to this {@link MetadataStore}s
     * collection of known {@link ConstraintCollection}s.
     */
    ConstraintCollection createConstraintCollection(String description, Experiment experiment, Target... scope);

    
    /**
     * This method creates a new {@link ConstraintCollection} that will also be added to this {@link MetadataStore}s
     * collection of known {@link ConstraintCollection}s.
     */
    ConstraintCollection createConstraintCollection(String description, Target... scope);

    
    /**
     * Returns the {@link de.hpi.isg.mdms.model.util.IdUtils} of this store.
     * 
     * @return the {@link de.hpi.isg.mdms.model.util.IdUtils}
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
     * {@link ConstraintCollection} and containing {@link de.hpi.isg.mdms.model.constraints.Constraint}s will be deleted that have the {@link Schema} or a
     * child element in it's scope.
     * 
     * @param schema
     *        the {@link Schema} to remove
     */
    public void removeSchema(Schema schema);

    /**
     * Removes the {@link ConstraintCollection} from the store as well as all {@link de.hpi.isg.mdms.model.constraints.Constraint}.
     * 
     * @param constraintCollection
     */
    public void removeConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * Removes an {@link Algorithm} and ALL child {@link Experiment} objects from the store. Also ALL
     * {@link ConstraintCollection} connected to these {@link Experiment} and containing {@link de.hpi.isg.mdms.model.constraints.Constraint}s will be deleted. 
     * 
     * @param algorithm
     *        the {@link Algorithm} to remove
     */
    public void removeAlgorithm(Algorithm algorithm);

    
    
    /**
     * Removes a {@link Experiment} and ALL child {@link ConstraintCollection} objects from the store. Also ALL
     * containing {@link de.hpi.isg.mdms.model.constraints.Constraint}s will be deleted.
     * 
     * @param experiment
     *        the {@link Experiment} to remove
     */
    public void removeExperiment(Experiment experiment);

    
    /**
     * Retrieve an experiment from the store if it exists for the given id
     *
     * @param experimentId
     * @return {@link Experiment}
     */
	public Experiment getExperimentById(int experimentId);

}
