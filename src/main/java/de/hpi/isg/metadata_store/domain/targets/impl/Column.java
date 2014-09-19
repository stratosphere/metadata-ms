package de.hpi.isg.metadata_store.domain.targets.impl;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.common.MyObserver;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.IColumn;

public class Column extends AbstractTarget implements IColumn {
	
	private static final long serialVersionUID = 6715373932483124301L;

	private Column(MyObserver observer, long id, String name, ILocation location) {
		super(observer, id, name, location);
	}
	
	public static Column buildAndRegister(MyObserver observer, long id, String name, ILocation location){
		Column newColumn = new Column(observer, id, name, location);
		newColumn.notifyListeners();
		return newColumn;
	}

	@Override
	public String toString() {
		return "Column [getLocation()="
				+ getLocation() + ", getId()=" + getId() + ", getName()="
				+ getName() + "]";
	}
}
