package de.hpi.isg.metadata_store.domain.targets.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.Column;

public class DefaultColumn extends AbstractTarget implements Column {

    private static final long serialVersionUID = 6715373932483124301L;

    private DefaultColumn(Observer observer, long id, String name, Location location) {
	super(observer, id, name, location);
    }

    public static DefaultColumn buildAndRegister(Observer observer, long id, String name, Location location) {
	DefaultColumn newColumn = new DefaultColumn(observer, id, name, location);
	newColumn.notifyListeners();
	return newColumn;
    }

    @Override
    public String toString() {
	return "Column [getLocation()=" + getLocation() + ", getId()=" + getId() + ", getName()=" + getName() + "]";
    }
}
