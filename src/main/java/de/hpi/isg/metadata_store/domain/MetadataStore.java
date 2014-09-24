package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.targets.Schema;

public interface MetadataStore extends Serializable, Observer {
    public void addConstraint(Constraint constraint);

    public void addSchema(Schema schema);

    public Collection<Target> getAllTargets();

    public Collection<Constraint> getConstraints();

    public Collection<Schema> getSchemas();
}
