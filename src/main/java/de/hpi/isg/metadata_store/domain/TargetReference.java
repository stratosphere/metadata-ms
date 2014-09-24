package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

/**
 * A {@link TargetReference} stores the information about {@link Target}s for
 * which a certains {@link Constraint} holds.
 *
 */

public interface TargetReference extends Serializable {
    public Collection<Target> getAllTargets();
}
