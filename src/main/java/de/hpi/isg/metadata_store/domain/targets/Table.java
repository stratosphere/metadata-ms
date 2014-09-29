package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;

/**
 * A {@link Table} represents a table inside of a {@link Schema} and consists of one or more {@link Column}s.
 */

public interface Table extends Target {

	/**
	 * @deprecated use {@link #addColumn(MetadataStore, String, int)} instead
	 * @param column
	 * @return
	 */
	public Table addColumn(Column column);

	/**
	 * Adds a new column to this table.
	 * 
	 * @param metadataStore
	 *            is the metadata store in which the new column shall be stored
	 * @param name
	 *            is the name of the new column
	 * @param index
	 *            is the index of the column within this table
	 * @return the added column
	 */
	public Column addColumn(MetadataStore metadataStore, String name, int index);

	public Collection<Column> getColumns();

	/**
	 * @return the parent schema of this table
	 */
	public Schema getSchema();
}
