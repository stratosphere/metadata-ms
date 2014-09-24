package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

public interface TargetReference extends Serializable {
    public Collection<Target> getAllTargets();
}
