package de.hpi.isg.metadata_store.domain.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.Observable;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;

public abstract class AbstractTarget extends AbstractIdentifiableAndNamed implements Target, Observable {

    private static final long serialVersionUID = -583488154227852034L;

    @ExcludeHashCodeEquals
    private final Observer observer;

    private final Location location;

    public AbstractTarget(Observer observer, int id, String name, Location location) {
	super(id, name);
	this.location = location;
	this.observer = observer;
    }

    @Override
    public Location getLocation() {
	return this.location;
    }

    protected Observer getObserver() {
	return this.observer;
    }

    @Override
    public void notifyListeners() {
	this.observer.update(this);
    }
}
