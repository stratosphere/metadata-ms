package de.hpi.isg.mdms.model.targets;

import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.common.Printable;

/**
 * The default implementation of the {@link de.hpi.isg.mdms.model.targets.Column}.
 *
 */

public class DefaultColumn extends AbstractTarget implements Column {
    @Printable
    private final Location location;

    public static Column buildAndRegister(final Observer observer, final Table table, final int id, final String name,
            final String description,
            final Location location) {
        final DefaultColumn newColumn = new DefaultColumn(observer, table, id, name, description, location);
        newColumn.register();
        return newColumn;
    }

    public static Column buildAndRegister(final Observer observer, final Table table, final String name,
            final String description,
            final Location location) {
        final DefaultColumn newColumn = new DefaultColumn(observer, table, -1, name, description, location);
        newColumn.register();
        return newColumn;
    }

    private static final long serialVersionUID = 6715373932483124301L;

    private final Table table;

    private DefaultColumn(final Observer observer, final Table table, final int id, final String name,
            String description,
            final Location location) {
        super(observer, id, name, description, location);
        this.location = location;
        this.table = table;
    }

    /**
     * @return the parent table
     */
    @Override
    public Table getTable() {
        return this.table;
    }

    @Override
    public String getNameWithTableName() {
        return getTable().getName() + "." + getName();
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return String.format("Column[%s, %08x]", getNameWithTableName(), getId());

    }
}
