package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.MyObserver;
import de.hpi.isg.metadata_store.domain.common.Named;
import de.hpi.isg.metadata_store.domain.targets.ISchema;

public interface IMetadataStore extends Identifiable, Named, Serializable, MyObserver{
	public Collection<ISchema> getSchemas();
	public Collection<IConstraint> getConstraints();
	public void addConstraint(IConstraint constraint);
}
