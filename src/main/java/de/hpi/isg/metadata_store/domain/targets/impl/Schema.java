package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.impl.AbstractTarget;
import de.hpi.isg.metadata_store.domain.targets.ISchema;
import de.hpi.isg.metadata_store.domain.targets.ITable;

public class Schema extends AbstractTarget implements ISchema {

	private static final long serialVersionUID = 8383281581697630605L;

	private Collection<ITable> tables;

	public Schema(long id, String name, ILocation location) {
		super(id, name, location);
		this.tables = new HashSet<ITable>();
	}

	@Override
	public Collection<ITable> getTables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISchema addTable(ITable table) {
		this.tables.add(table);
		return this;
	}
}
