package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

public interface ITable {
    public Collection<IColumn> getColumns();

    public ITable addColumn(IColumn column);
}
