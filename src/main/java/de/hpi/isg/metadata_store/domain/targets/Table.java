package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

public interface Table {
    public Collection<Column> getColumns();

    public Table addColumn(Column column);
}
