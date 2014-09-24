package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.Named;
import de.hpi.isg.metadata_store.domain.targets.Schema;

public interface MetadataStore extends Identifiable, Named, Serializable, Observer {
    public Collection<Schema> getSchemas();

    public Collection<Constraint> getConstraints();

    public void addConstraint(Constraint constraint);

    public Collection<Target> getAllTargets();
}
