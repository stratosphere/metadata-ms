package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * The default implementation of the {@link Schema}.
 *
 */
public class DefaultSchema extends AbstractTarget implements Schema {

    public static Schema buildAndRegister(Observer observer, int id, String name, Location location) {
	final DefaultSchema newSchema = new DefaultSchema(observer, id, name, location);
	newSchema.notifyObserver();
	return newSchema;
    }

    public static Schema buildAndRegister(Observer observer, String name, Location location) {
	final DefaultSchema newSchema = new DefaultSchema(observer, -1, name, location);
	newSchema.notifyObserver();
	return newSchema;
    }

    private static final long serialVersionUID = 8383281581697630605L;

    @ExcludeHashCodeEquals
    private final Collection<Table> tables;

    private DefaultSchema(Observer observer, int id, String name, Location location) {
	super(observer, id, name, location);
	this.tables = new HashSet<Table>();
    }

    @Override
    public Schema addTable(Table table) {
	this.tables.add(table);
	return this;
    }
    
    @Override
    public Table addTable(MetadataStore metadataStore, String name, Location location) {
    	Validate.isTrue(metadataStore.getSchemas().contains(this));
    	int tableId = metadataStore.getUnusedTableId(this);
    	Table table = DefaultTable.buildAndRegister(metadataStore, this, tableId, name, location);
    	addTable(table);
    	return table;
    }
    
    @Override
    public Collection<Table> getTables() {
	return this.tables;
    }

    @Override
    public String toString() {
	return "Schema [tables=" + this.tables + ", getTables()=" + this.getTables() + ", getLocation()="
		+ this.getLocation() + ", getId()=" + this.getId() + ", getName()=" + this.getName() + "]";
    }
}
