package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;

import java.util.Collection;
import java.util.List;

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
     * @param constraint      the constraint to serialize
     * @param constraintCollection the constraint collection to which the constraint belongs
     */
    void serialize(Constraint constraint, ConstraintCollection constraintCollection);

    /**
     * Retrieves and deserializes all Constraints of a given {@link ConstraintCollection}. If the constraintCollection
     * parameter is null all {@link Constraint}s of this type will be retrieved.
     *
     * @param constraintCollection
     *        , or null if all {@link Constraint}s shall be retrieved
     * @return The collection of all {@link javax.swing.SpringLayout.Constraints} of a particular {@link ConstraintCollection}, or all
     *         constraints if no was specified.
     */
    Collection<T> deserializeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection);

    /**
     * Removes all {@link Constraint}s of the provided {@link ConstraintCollection}.
     * 
     * @param constraintCollection is the constraint collection from that constraints are to be removed
     */
    void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection);

}
