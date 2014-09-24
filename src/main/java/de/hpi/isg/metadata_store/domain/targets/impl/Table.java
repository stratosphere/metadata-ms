package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.common.MyObserver;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.IColumn;
import de.hpi.isg.metadata_store.domain.targets.ITable;

public class Table extends AbstractTarget implements ITable {

    private static final long serialVersionUID = 1695408629652071459L;
    @ExcludeHashCodeEquals
    private Collection<IColumn> columns;

    private Table(MyObserver observer, long id, String name, ILocation location) {
	super(observer, id, name, location);
	this.columns = new HashSet<IColumn>();
    }

    public static Table buildAndRegister(MyObserver observer, long id, String name, ILocation location) {
	Table newTable = new Table(observer, id, name, location);
	newTable.notifyListeners();
	return newTable;
    }

    @Override
    public Collection<IColumn> getColumns() {
	return columns;
    }

    @Override
    public ITable addColumn(IColumn column) {
	this.columns.add(column);
	return this;
    }

    @Override
    public String toString() {
	return "Table [columns=" + columns + ", getColumns()=" + getColumns() + ", getLocation()=" + getLocation()
		+ ", getId()=" + getId() + ", getName()=" + getName() + "]";
    }
}
