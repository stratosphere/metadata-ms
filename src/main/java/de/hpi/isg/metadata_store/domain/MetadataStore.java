package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.targets.Schema;

/**
 * A {@link MetadataStore} stores schema information as well as
 * {@link Constraint}s holding on the objects stored in it.
 *
 */

public interface MetadataStore extends Serializable, Observer {
    public void addConstraint(Constraint constraint);

    public void addSchema(Schema schema);

    public Collection<Target> getAllTargets();

    public Collection<Constraint> getConstraints();

    public Collection<Schema> getSchemas();
}
