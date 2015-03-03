package de.hpi.isg.mdms.domain;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.mdms.domain.common.Identifiable;
import de.hpi.isg.mdms.domain.common.Named;
import de.hpi.isg.mdms.domain.common.Observer;
import de.hpi.isg.mdms.domain.targets.Column;
import de.hpi.isg.mdms.domain.targets.Schema;
import de.hpi.isg.mdms.domain.targets.Table;
import de.hpi.isg.mdms.domain.targets.impl.RDBMSTable;

/**
 * Every physical data store object is represented as a {@link Target}. So {@link Schema}s, {@link Table}s, and
 * {@link Column}s are {@link Target}s that can be referenced in {@link Constraint}s via {@link TargetReference}s.
 *
 */

public interface Target extends Identifiable, Named, Serializable, Described {
    /**
     * Returns the {@link Location} of a {@link Target}.
     * 
     * @return the {@link Location}
     */
    public Location getLocation();

    /**
     * This functions calls the {@link Observer} of this {@link Target} to register itself to the {@link Collection} of
     * known targets. Is usually called by the factory methdods of {@link Target} implementations, e.g.
     * {@link RDBMSTable#buildAndRegisterAndAdd(de.hpi.isg.mdms.domain.impl.RDBMSMetadataStore, Schema, int, String, String, Location)}
     */
    public void register();
}
