package de.hpi.isg.mdms.model.targets;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.mdms.model.common.Described;
import de.hpi.isg.mdms.model.common.Identifiable;
import de.hpi.isg.mdms.model.common.Named;
import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.location.Location;

/**
 * Every physical data store object is represented as a {@link Target}. So {@link de.hpi.isg.mdms.model.targets.Schema}s, {@link de.hpi.isg.mdms.model.targets.Table}s, and
 * {@link Column}s are {@link Target}s that can be referenced in {@link de.hpi.isg.mdms.model.constraints.Constraint}s via {@link TargetReference}s.
 *
 */

public interface Target extends Identifiable, Named, Serializable, Described {
    /**
     * Returns the {@link de.hpi.isg.mdms.model.location.Location} of a {@link Target}.
     * 
     * @return the {@link de.hpi.isg.mdms.model.location.Location}
     */
    public Location getLocation();

    /**
     * This functions calls the {@link Observer} of this {@link Target} to register itself to the {@link Collection} of
     * known targets. Is usually called by the factory methdods of {@link Target} implementations, e.g.
     * {@link RDBMSTable#buildAndRegisterAndAdd(de.hpi.isg.mdms.domain.impl.RDBMSMetadataStore, de.hpi.isg.mdms.model.targets.Schema, int, String, String, Location)}
     */
    public void register();
}
