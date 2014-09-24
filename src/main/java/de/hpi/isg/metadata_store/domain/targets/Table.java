package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Target;

/**
 * A {@link Table} represents a table inside of a {@link Schema} and consists of
 * one or more {@link Column}s.
 *
 */

public interface Table extends Target {
    public Table addColumn(Column column);

    public Collection<Column> getColumns();
}
