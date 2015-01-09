package de.hpi.isg.metadata_store.domain;

import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.Serializable;

/**
 * A {@link TargetReference} stores the information about {@link Target}s for which a certains {@link Constraint} holds.
 *
 */

public interface TargetReference extends Serializable {
    
    public IntCollection getAllTargetIds();
}
