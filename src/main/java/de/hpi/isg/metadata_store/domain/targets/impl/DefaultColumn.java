package de.hpi.isg.metadata_store.domain.targets.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Table;

/**
 * The default implementation of the {@link Column}.
 *
 */

public class DefaultColumn extends AbstractTarget implements Column {

    public static Column buildAndRegister(Observer observer, Table table, int id, String name, Location location) {
	final DefaultColumn newColumn = new DefaultColumn(observer, table, id, name, location);
	newColumn.notifyObserver();
	return newColumn;
    }

    public static Column buildAndRegister(Observer observer, Table table, String name, Location location) {
	final DefaultColumn newColumn = new DefaultColumn(observer, table, -1, name, location);
	newColumn.notifyObserver();
	return newColumn;
    }

    private static final long serialVersionUID = 6715373932483124301L;
    
    private final Table table;

    private DefaultColumn(Observer observer, Table table, int id, String name, Location location) {
	super(observer, id, name, location);
	this.table = table;
    }

    
	/**
	 * @return the parent table
	 */
	public Table getTable() {
		return table;
	}
	
	@Override
	public String getNameWithTableName() {
		return getTable().getName() + "." + getName();
	}
    
    @Override
    public String toString() {
    	return String.format("Column[%s, %08x]", getNameWithTableName(), getId());

    }
}
