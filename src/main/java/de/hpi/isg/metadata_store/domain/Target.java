package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * Every physical data store object is represented as a {@link Target}. So {@link Schema}s, {@link Table}s, and
 * {@link Column}s are {@link Target}s that can be referenced in {@link Constraint}s via {@link TargetReference}s.
 *
 */

public interface Target extends Identifiable, Named, Serializable, Described {
    public Location getLocation();

    public void register();
}
