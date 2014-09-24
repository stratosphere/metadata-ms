package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

public interface ITargetReference extends Serializable {
    public Collection<ITarget> getAllTargets();
}
