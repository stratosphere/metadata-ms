package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.IColumn;
import de.hpi.isg.metadata_store.domain.targets.ITable;

public class Table extends AbstractTarget implements ITable {

	private static final long serialVersionUID = 1695408629652071459L;
	private Collection<IColumn> columns;

	public Table(long id, String name, ILocation location) {
		super(id, name, location);
		this.columns = new HashSet<IColumn>();
	}

	@Override
	public Collection<IColumn> getColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITable addColumn(IColumn column) {
		this.columns.add(column);
		return this;
	}
}
