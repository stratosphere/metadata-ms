package de.hpi.isg.metadata_store.domain.targets;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.ITarget;

public interface ISchema extends ITarget{
	public Collection<ITable> getTables();
	public ISchema addTable(ITable table);
}
