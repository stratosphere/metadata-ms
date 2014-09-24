package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface is the supertype for all kinds of Constraints that can hold on data represented
 */
import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;

public interface Constraint extends Identifiable, Named, Serializable {

    public Map<Object, Object> getProperties();

    public TargetReference getTargetReference();

}
