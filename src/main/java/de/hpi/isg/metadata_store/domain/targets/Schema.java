package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Target;

/**
 * A {@link Schema} combines multiple corresponding {@link Table}s.
 *
 */

public interface Schema extends Target {
    public Schema addTable(Table table);

    public Collection<Table> getTables();
}
