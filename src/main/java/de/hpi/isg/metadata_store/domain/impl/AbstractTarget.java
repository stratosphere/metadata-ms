package de.hpi.isg.metadata_store.domain.impl;

import java.io.Serializable;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.ITarget;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;

public abstract class AbstractTarget extends AbstractIdentifiableAndNamed
		implements ITarget, Serializable {

	private static final long serialVersionUID = -583488154227852034L;

	private ILocation location;

	public AbstractTarget(long id, String name, ILocation location) {
		super(id, name);
		this.location = location;
	}

	@Override
	public ILocation getLocation() {
		return location;
	}

	public void setLocation(ILocation location) {
		this.location = location;
	}

}
