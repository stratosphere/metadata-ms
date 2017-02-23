package de.hpi.isg.mdms.model.targets;

import de.hpi.isg.mdms.model.location.Location;

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

    @Override
    default Target.Type getType() {
        return Target.Type.COLUMN;
    }

}
