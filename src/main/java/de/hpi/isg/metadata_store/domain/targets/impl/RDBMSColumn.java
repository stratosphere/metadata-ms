package de.hpi.isg.metadata_store.domain.targets.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractRDBMSTarget;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * The default implementation of the {@link Column}.
 *
 */

public class RDBMSColumn extends AbstractRDBMSTarget implements Column {

    private static final long serialVersionUID = 2505519123200337186L;

    @ExcludeHashCodeEquals
    private final Table table;

    public static Column buildAndRegisterAndAdd(final RDBMSMetadataStore observer, final Table table, final int id,
            final String name,
            final Location location) {
        final RDBMSColumn newColumn = new RDBMSColumn(observer, table, id, name, location);
        newColumn.register();
        newColumn.getSqlInterface().addColumnToTable(newColumn, table);
        return newColumn;
    }

    public static Column buildAndRegisterAndAdd(final RDBMSMetadataStore observer, final Table table,
            final String name,
            final Location location) {
        final RDBMSColumn newColumn = new RDBMSColumn(observer, table, -1, name, location);
        newColumn.register();
        newColumn.getSqlInterface().addColumnToTable(newColumn, table);
        return newColumn;
    }

    public static Column build(final RDBMSMetadataStore observer, final Table table, final int id,
            final String name,
            final Location location) {
        final RDBMSColumn newColumn = new RDBMSColumn(observer, table, id, name, location);
        return newColumn;
    }

    private RDBMSColumn(final RDBMSMetadataStore observer, final Table table, final int id, final String name,
            final Location location) {
        super(observer, id, name, location);
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
    public String toString() {
        return String.format("Column[%s, %08x]", getNameWithTableName(), getId());

    }
}
