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

    public static DefaultTable buildAndRegister(Observer observer, int id, String name, Location location) {
	final DefaultTable newTable = new DefaultTable(observer, id, name, location);
	newTable.notifyListeners();
	return newTable;
    }

    private static final long serialVersionUID = 1695408629652071459L;

    @ExcludeHashCodeEquals
    private final Collection<Column> columns;

    private DefaultTable(Observer observer, int id, String name, Location location) {
	super(observer, id, name, location);
	this.columns = new HashSet<Column>();
    }

    @Override
    public Table addColumn(Column column) {
	this.columns.add(column);
	return this;
    }

    @Override
    public Collection<Column> getColumns() {
	return this.columns;
    }

    @Override
    public String toString() {
	return "Table [columns=" + this.columns + ", getColumns()=" + this.getColumns() + ", getLocation()="
		+ this.getLocation() + ", getId()=" + this.getId() + ", getName()=" + this.getName() + "]";
    }
}
