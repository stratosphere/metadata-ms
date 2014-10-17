package de.hpi.isg.metadata_store.domain.factories;

import java.sql.SQLException;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Schema;

/**
 * This interface describes common functionalities that a RDBMS-specifc interface for a {@link RDBMSMetadataStore} must
 * provide.
 * 
 * @author fabian
 *
 */
public interface SQLInterface {

    public void initializeMetadataStore();

    public boolean cotainsTarget(Target target);

    public void addConstraint(Constraint constraint);

    public void addSchema(Schema schema);

    public Collection<? extends Target> getAllTargets();

    public Collection<? extends Constraint> getAllConstraints();

    public Collection<Schema> getAllSchemas();

    public Collection<Integer> getIdsInUse();

    public boolean addToIdsInUse(int id);

    public void addTarget(Target target);

    public Collection<ConstraintCollection> getAllConstraintCollections();

    public void addConstraintCollection(ConstraintCollection constraintCollection);
}
