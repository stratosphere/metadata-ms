package de.hpi.isg.mdms.model.targets;

import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.Serializable;
import java.util.Collection;

/**
 * A {@link TargetReference} stores the information about {@link Target}s for which a certains {@link de.hpi.isg.mdms.model.constraints.Constraint} holds.
 *
 */

public interface TargetReference extends Serializable {
    /**
     * Returns an {@link IntCollection} of all {@link Target} id's that are included in this {@link TargetReference}.
     * 
     * @return {@link Collection} of target id's.
     */
    public IntCollection getAllTargetIds();
}
