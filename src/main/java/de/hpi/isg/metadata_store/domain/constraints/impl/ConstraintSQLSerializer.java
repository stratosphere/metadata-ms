package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.util.Collection;
import java.util.List;

import javax.swing.SpringLayout.Constraints;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;

/**
 * {@link ConstraintSQLSerializer} provide the serializing and de-serializing functionality of {@link Constraint}s that
 * are needed if the Constraint are stored inside a {@link RDBMSMetadataStore}.
 * 
 * @author fabian
 *
 */
public interface ConstraintSQLSerializer<T extends Constraint> {

    /**
     * This funciton returns all table names that the {@link ConstraintSQLSerializer} uses for the storing of it's
     * constraint type.
     * 
     * @return
     */
    List<String> getTableNames();

    /**
     * This function instructs the {@link ConstraintSQLSerializer} to create all needed tables.
     */
    void initializeTables();

    /**
     * Serializes a constraint and stores it in the {@link RDBMSMetadataStore}.
     * 
     * @param constraintId
     *        the integer id that shall be used
     * @param constraint
     *        the constraint to serialize
     */
    void serialize(Integer constraintId, Constraint constraint);

    /**
     * Retrieves and deserializes all Constraints of a given {@link ConstraintCollection}. If the constraintCollection
     * parameter is null all {@link Constraint}s of this type will be retrieved.
     * 
     * @param constraintCollection
     *        , or null if all {@link Constraint}s shall be retrieved
     * @return The collection of all {@link Constraints} of a particular {@link ConstraintCollection}, or all
     *         constraints if no was specified.
     */
    Collection<T> deserializeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * Removes all {@link Constraint}s of the provided {@link ConstraintCollection}.
     * 
     * @param constraintCollection
     */
    void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection);

}
