package de.hpi.isg.mdms.domain.targets;

import de.hpi.isg.mdms.domain.Location;
import de.hpi.isg.mdms.domain.Target;

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

    Location getLocation();
}
