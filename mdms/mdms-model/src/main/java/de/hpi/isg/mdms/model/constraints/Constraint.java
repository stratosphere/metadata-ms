package de.hpi.isg.mdms.model.constraints;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Target;

import java.io.Serializable;

/**
 * While {@link MetadataStore}s should be able to hold all kind of data, this interface provides methods that
 * allow for improved management of this data.
 */
public interface Constraint extends Serializable {

    /**
     * Returns an {@link int[]} of all {@link Target} IDs that are referenced by this instance.
     *
     * @return the {@link Target} IDs
     */
    int[] getAllTargetIds();

}
