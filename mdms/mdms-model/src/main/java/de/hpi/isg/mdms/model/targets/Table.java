package de.hpi.isg.mdms.model.targets;

import java.util.Collection;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;

/**
 * A {@link Table} represents a table inside of a {@link Schema} and consists of one or more {@link Column}s.
 */

public interface Table extends Target {

    /**
     * Adds a new column to this table.
     *
     * @param metadataStore
     *        is the metadata store in which the new column shall be stored
     * @param name
     *        is the name of the new column
     * @param index
     *        is the index of the column within this table
     * @return the added column
     */
    Column addColumn(MetadataStore metadataStore, String name, String description, int index);

    Collection<Column> getColumns();

    Column getColumnByName(String name) throws NameAmbigousException;

    Collection<Column> getColumnsByName(String name);

    Column getColumnById(int id);

    /**
     * @return the parent schema of this table
     */
    Schema getSchema();

    @Override
    default Target.Type getType() {
        return Target.Type.TABLE;
    }
}
