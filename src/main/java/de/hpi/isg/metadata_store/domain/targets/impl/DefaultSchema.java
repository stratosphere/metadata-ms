package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;

public class DefaultSchema extends AbstractTarget implements Schema {

    private static final long serialVersionUID = 8383281581697630605L;
    @ExcludeHashCodeEquals
    private Collection<Table> tables;

    private DefaultSchema(Observer observer, long id, String name, Location location) {
	super(observer, id, name, location);
	this.tables = new HashSet<Table>();
    }

    public static DefaultSchema buildAndRegister(Observer observer, long id, String name, Location location) {
	DefaultSchema newSchema = new DefaultSchema(observer, id, name, location);
	newSchema.notifyListeners();
	return newSchema;
    }

    @Override
    public Collection<Table> getTables() {
	return tables;
    }

    @Override
    public Schema addTable(Table table) {
	this.tables.add(table);
	return this;
    }

    @Override
    public String toString() {
	return "Schema [tables=" + tables + ", getTables()=" + getTables() + ", getLocation()=" + getLocation()
		+ ", getId()=" + getId() + ", getName()=" + getName() + "]";
    }
}
