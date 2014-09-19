package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.common.MyObserver;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.ISchema;
import de.hpi.isg.metadata_store.domain.targets.ITable;

public class Schema extends AbstractTarget implements ISchema {

	private static final long serialVersionUID = 8383281581697630605L;

	private Collection<ITable> tables;

	private Schema(MyObserver observer, long id, String name, ILocation location) {
		super(observer, id, name, location);
		this.tables = new HashSet<ITable>();
	}

	public static Schema buildAndRegister(MyObserver observer, long id,
			String name, ILocation location) {
		Schema newSchema = new Schema(observer, id, name, location);
		newSchema.notifyListeners();
		return newSchema;
	}

	@Override
	public Collection<ITable> getTables() {
		return tables;
	}

	@Override
	public ISchema addTable(ITable table) {
		this.tables.add(table);
		return this;
	}

	@Override
	public String toString() {
		return "Schema [tables=" + tables + ", getTables()=" + getTables()
				+ ", getLocation()=" + getLocation() + ", getId()=" + getId()
				+ ", getName()=" + getName() + "]";
	}
}
