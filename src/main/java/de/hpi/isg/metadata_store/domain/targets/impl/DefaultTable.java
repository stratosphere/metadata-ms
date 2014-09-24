package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Table;

public class DefaultTable extends AbstractTarget implements Table {

    private static final long serialVersionUID = 1695408629652071459L;
    @ExcludeHashCodeEquals
    private Collection<Column> columns;

    private DefaultTable(Observer observer, long id, String name, Location location) {
	super(observer, id, name, location);
	this.columns = new HashSet<Column>();
    }

    public static DefaultTable buildAndRegister(Observer observer, long id, String name, Location location) {
	DefaultTable newTable = new DefaultTable(observer, id, name, location);
	newTable.notifyListeners();
	return newTable;
    }

    @Override
    public Collection<Column> getColumns() {
	return columns;
    }

    @Override
    public Table addColumn(Column column) {
	this.columns.add(column);
	return this;
    }

    @Override
    public String toString() {
	return "Table [columns=" + columns + ", getColumns()=" + getColumns() + ", getLocation()=" + getLocation()
		+ ", getId()=" + getId() + ", getName()=" + getName() + "]";
    }
}
