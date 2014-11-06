package de.hpi.isg.metadata_store.domain.targets;

import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;

/**
 * A {@link Column} represents one column in a {@link Table}.
 *
 */

public interface Column extends Target {

    /**
     * @return the parent table of this column
     */
    Table getTable();

    /**
     * @return the name of this column with the table name prepended
     */
    String getNameWithTableName();

    IndexedLocation getLocation();
}
