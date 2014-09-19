package de.hpi.isg.metadata_store.domain.impl;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.ITarget;
import de.hpi.isg.metadata_store.domain.common.MyObservable;
import de.hpi.isg.metadata_store.domain.common.MyObserver;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;

public abstract class AbstractTarget extends AbstractIdentifiableAndNamed
		implements ITarget, MyObservable {

	private static final long serialVersionUID = -583488154227852034L;

	@ExcludeHashCodeEquals
	private final MyObserver observer;

	private final ILocation location;

	public AbstractTarget(MyObserver observer, long id, String name,
			ILocation location) {
		super(id, name);
		this.location = location;
		this.observer = observer;
	}

	protected MyObserver getObserver() {
		return observer;
	}

	@Override
	public ILocation getLocation() {
		return location;
	}

	@Override
	public void notifyListeners() {
		this.observer.update(this);
	}
}
