package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Target;

public interface Schema extends Target {
    public Collection<Table> getTables();

    public Schema addTable(Table table);
}
