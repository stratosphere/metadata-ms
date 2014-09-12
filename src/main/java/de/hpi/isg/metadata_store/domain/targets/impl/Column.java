package de.hpi.isg.metadata_store.domain.targets.impl;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.IColumn;

public class Column extends AbstractTarget implements IColumn {
	
	private static final long serialVersionUID = 6715373932483124301L;

	public Column(long id, String name, ILocation location) {
		super(id, name, location);
	}
}
