package de.hpi.isg.mdms.model;

import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * A {@link MetadataStore} stores schema information as well as {@link de.hpi.isg.mdms.model.constraints.Constraint}s holding on the objects stored in it.
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
    Schema addSchema(String name, String description, Location location);

    /**
     * Checks whether this store includes a {@link Target} with that id.
     *
     * @param id
     * @return true if target with id is contained, else false.
     */
    boolean hasTargetWithId(int id);

    /**
     * Returns a {@link Collection} of all {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}s.
     *
     * @return all {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}s.
     */
    Collection<ConstraintCollection<? extends Constraint>> getConstraintCollections();

    /**
     * Returns a particular {@link ConstraintCollection} with the given id.
     *
     * @param id
     * @return the {@link ConstraintCollection} with the given id, <code>null</code> if no exists with given id.
     */
    ConstraintCollection<? extends Constraint> getConstraintCollection(int id);

    /**
     * Retrieve all {@link ConstraintCollection}s in this instance that have the given scope.
     *
     * @param scope the scope {@link Target}
     * @return the matching {@link ConstraintCollection}s
     */
    default Collection<ConstraintCollection<? extends Constraint>> getConstraintCollectionByTarget(Target scope) {
        Collection<ConstraintCollection<? extends Constraint>> result = new LinkedList<>();
        for (ConstraintCollection<? extends Constraint> constraintCollection : this.getConstraintCollections()) {
            for (Target ccScope : constraintCollection.getScope())
                if (this.getIdUtils().isContained(scope.getId(), ccScope.getId())) {
                    result.add(constraintCollection);
                    break;
                }
        }
        return result;
    }

    /**
     * Retrieve all {@link ConstraintCollection}s in this instance that have the given {@link Constraint} type.
     *
     * @param constrainttype the {@link Class} of the {@link Constraint} type
     * @return the matching {@link ConstraintCollection}s
     */
    @SuppressWarnings("unchecked") // We check by hand.
    default <T extends Constraint> Collection<ConstraintCollection<T>> getConstraintCollectionByConstraintType(Class<T> constrainttype) {
        Collection<ConstraintCollection<T>> result = new LinkedList<>();
        for (ConstraintCollection<? extends Constraint> constraintCollection : this.getConstraintCollections()) {
            if (constraintCollection.getConstraintClass() == constrainttype) {
                result.add((ConstraintCollection<T>) constraintCollection);
                continue;
            }
        }
        return result;
    }

    /**
     * Retrieve all {@link ConstraintCollection}s in this instance that have the given {@link Constraint} type and scope.
     *
     * @param constrainttype the {@link Class} of the {@link Constraint} type
     * @param scope          the scope {@link Target}
     * @return the matching {@link ConstraintCollection}s
     */
    @SuppressWarnings("unchecked") // We check by hand.
    default <T extends Constraint> Collection<ConstraintCollection<T>>
    getConstraintCollectionByConstraintTypeAndScope(Class<T> constrainttype, Target scope) {
        Collection<ConstraintCollection<T>> result = new LinkedList<>();
        for (ConstraintCollection<? extends Constraint> constraintCollection : this.getConstraintCollections()) {
            if (constraintCollection.getConstraintClass() == constrainttype) {
                for (Target ccScope : constraintCollection.getScope())
                    if (this.getIdUtils().isContained(scope.getId(), ccScope.getId())) {
                        result.add((ConstraintCollection<T>) constraintCollection);
                        break;
                    }
            }
        }
        return result;
    }

    /**
     * Retrieve all {@link ConstraintCollection}s in this instance that have the given {@link Constraint} type and whose
     * scope is included in the given {@link Target}.
     *
     * @param constrainttype the {@link Class} of the {@link Constraint} type
     * @param target         the {@link Target}
     * @return the matching {@link ConstraintCollection}s
     */
    @SuppressWarnings("unchecked") // We check by hand.
    default <T extends Constraint> Collection<ConstraintCollection<T>>
    getIncludedConstraintCollections(Class<T> constrainttype, Target target) {
        Collection<ConstraintCollection<T>> result = new LinkedList<>();
        NextConstraintCollection:
        for (ConstraintCollection<? extends Constraint> constraintCollection : this.getConstraintCollections()) {
            if (constraintCollection.getConstraintClass() == constrainttype) {
                for (Target ccScope : constraintCollection.getScope())
                    if (!this.getIdUtils().isContained(ccScope.getId(), target.getId()))
                        continue NextConstraintCollection;
                result.add((ConstraintCollection<T>) constraintCollection);
            }
        }
        return result;
    }

    /**
     * Retrieve a schema from the store if it exists, throws {@link NameAmbigousException} if there are more than one
     * with that name
     *
     * @param schemaName
     * @return
     */
    Schema getSchemaByName(String schemaName) throws NameAmbigousException;

    /**
     * Retrieve a {@link Collection} of schemas from the store for the given name
     *
     * @param schemaName
     * @return
     */
    Collection<Schema> getSchemasByName(String schemaName);

    /**
     * Retrieve a schema from the store if it exists for the given id
     *
     * @param schemaId
     * @return
     */
    default Schema getSchemaById(int schemaId) {
        return (Schema) this.getTargetById(schemaId);
    }

    /**
     * Retrieve the {@link Target} with the given ID.
     *
     * @param targetId the ID of the {@link Target}
     * @return the {@link Target} or {@code null} if there is no match
     */
    Target getTargetById(int targetId);

    /**
     * Get all knwon {@link Schema}s.
     *
     * @return
     */
    Collection<Schema> getSchemas();

    /**
     * Resolves a {@link Target} by its name.
     *
     * @param targetName the name of the {@link Target} conforming to the regex {@code <schema>( "." <table> ( "." <column>)? )?}
     * @return the resolved {@link Target} or {@code null} if it could not be resolved
     * @throws NameAmbigousException if more than one {@link Target} matches
     */
    default Target getTargetByName(String targetName) throws NameAmbigousException {
        Target lastEncounteredTarget = null;
        int separatorIndex = -1;
        do {
            separatorIndex = targetName.indexOf('.', separatorIndex + 1);
            if (separatorIndex == -1) separatorIndex = targetName.length();
            String schemaName = targetName.substring(0, separatorIndex);
            Schema schema = this.getSchemaByName(schemaName);
            if (schema != null) {
                if (separatorIndex == targetName.length()) {
                    if (lastEncounteredTarget != null) throw new NameAmbigousException(targetName);
                    lastEncounteredTarget = schema;
                } else {
                    Target target = schema.getTargetByName(targetName.substring(separatorIndex + 1));
                    if (target != null) {
                        if (lastEncounteredTarget != null) throw new NameAmbigousException(targetName);
                        lastEncounteredTarget = target;
                    }
                }
            }
        } while (separatorIndex < targetName.length());
        return lastEncounteredTarget;
    }

    /**
     * Resolves a {@link Table} by its name.
     *
     * @param targetName the name of the {@link Table} conforming to the format {@code <schema> "." <table>}
     * @return the resolved {@link Table} or {@code null} if it could not be resolved
     * @throws NameAmbigousException    if more than one {@link Target} matches
     * @throws IllegalArgumentException if the matching {@link Target} is not a {@link Table}
     */
    default Table getTableByName(String targetName) throws IllegalArgumentException {
        Target target = this.getTargetByName(targetName);
        if (target != null && !(target instanceof Table)) {
            throw new IllegalArgumentException(String.format("%s is not a table.", target));
        }
        return (Table) target;
    }

    /**
     * Resolves a {@link Column} by its name.
     *
     * @param targetName the name of the {@link Table} conforming to the format {@code <schema> "." <table> "." <column>}
     * @return the resolved {@link Table} or {@code null} if it could not be resolved
     * @throws NameAmbigousException    if more than one {@link Target} matches
     * @throws IllegalArgumentException if the matching {@link Target} is not a {@link Column}
     */
    default Column getColumnByName(String targetName) throws IllegalArgumentException {
        Target target = this.getTargetByName(targetName);
        if (!(target instanceof Column)) {
            throw new IllegalArgumentException(String.format("%s is not a column.", target));
        }
        return (Column) target;
    }

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
     * @param schema is the schema to which the new table shall be added
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
    Algorithm getAlgorithmById(int algorithmId);

    /**
     * Retrieve an algorithm from the store if it exists for the given name
     *
     * @param name
     * @return {@link Algorithm}
     */
    Algorithm getAlgorithmByName(String name);


    /**
     * Get all knwon {@link Algorithm}s.
     *
     * @return {@link Collection} of {@link Algorithm}s.
     */
    Collection<Algorithm> getAlgorithms();


    /**
     * Get all knwon {@link Experiment}s.
     *
     * @return {@link Collection} of {@link Experiment}s.
     */
    Collection<Experiment> getExperiments();


    /**
     * This method creates a new {@link Experiment} that will also be added to this {@link MetadataStore}s
     * collection of known {@link Experiment}s.
     */
    Experiment createExperiment(String description, Algorithm algorithm);

    /**
     * This method creates a new {@link ConstraintCollection} that will also be added to this {@link MetadataStore}s
     * collection of known {@link ConstraintCollection}s.
     */
    <T extends Constraint> ConstraintCollection<T> createConstraintCollection(String description, Experiment experiment, Class<T> cls, Target... scope);

    /**
     * This method creates a new {@link ConstraintCollection} that will also be added to this {@link MetadataStore}s
     * collection of known {@link ConstraintCollection}s.
     */
    <T extends Constraint> ConstraintCollection<T> createConstraintCollection(String description, Class<T> cls, Target... scope);


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
    void save(String path) throws IOException;

    /**
     * Saves any pending changes in the metadata store.
     *
     * @throws Exception if the saving fails
     */
    void flush() throws Exception;

    /**
     * Removes a {@link Schema} and ALL child {@link Target} objects from the store. Also ALL
     * {@link ConstraintCollection} and containing {@link de.hpi.isg.mdms.model.constraints.Constraint}s will be deleted that have the {@link Schema} or a
     * child element in it's scope.
     *
     * @param schema the {@link Schema} to remove
     */
    void removeSchema(Schema schema);

    /**
     * Removes the {@link ConstraintCollection} from the store as well as all {@link de.hpi.isg.mdms.model.constraints.Constraint}.
     *
     * @param constraintCollection
     */
    void removeConstraintCollection(ConstraintCollection<? extends Constraint> constraintCollection);

    /**
     * Closes the MetadataStore.
     */
    void close();

    /**
     * Removes an {@link Algorithm} and ALL child {@link Experiment} objects from the store. Also ALL
     * {@link ConstraintCollection} connected to these {@link Experiment} and containing {@link de.hpi.isg.mdms.model.constraints.Constraint}s will be deleted.
     *
     * @param algorithm the {@link Algorithm} to remove
     */
    void removeAlgorithm(Algorithm algorithm);


    /**
     * Removes a {@link Experiment} and ALL child {@link ConstraintCollection} objects from the store. Also ALL
     * containing {@link de.hpi.isg.mdms.model.constraints.Constraint}s will be deleted.
     *
     * @param experiment the {@link Experiment} to remove
     */
    void removeExperiment(Experiment experiment);


    /**
     * Retrieve an experiment from the store if it exists for the given id
     *
     * @param experimentId
     * @return {@link Experiment}
     */
    Experiment getExperimentById(int experimentId);
}
