package de.hpi.isg.metadata_store.domain.targets.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.impl.Printable;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * The default implementation of the {@link Column}.
 *
 */

public class DefaultColumn extends AbstractTarget implements Column {
    @Printable
    private final Location location;

    public static Column buildAndRegister(final Observer observer, final Table table, final int id, final String name,
            final Location location) {
        final DefaultColumn newColumn = new DefaultColumn(observer, table, id, name, location);
        newColumn.register();
        return newColumn;
    }

    public static Column buildAndRegister(final Observer observer, final Table table, final String name,
            final Location location) {
        final DefaultColumn newColumn = new DefaultColumn(observer, table, -1, name, location);
        newColumn.register();
        return newColumn;
    }

    private static final long serialVersionUID = 6715373932483124301L;

    private final Table table;

    private DefaultColumn(final Observer observer, final Table table, final int id, final String name,
            final Location location) {
        super(observer, id, name, location);
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
