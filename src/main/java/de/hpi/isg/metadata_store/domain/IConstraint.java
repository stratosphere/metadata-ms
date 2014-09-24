package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Map;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;

public interface IConstraint extends Identifiable, Named, Serializable {

    public Map<Object, Object> getProperties();

    public ITargetReference getTargetReference();

}
