package de.hpi.isg.metadata_store.domain.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;

/**
 * {@link AbstractTarget} is a convenience class for all {@link Target}
 * implementation already taking car of {@link Location} and holding the
 * observing {@link MetadataStore} that acts as an {@link Observer} where new
 * targets have to be registered right after creation.
 *
 */

public abstract class AbstractTarget extends AbstractIdentifiableAndNamed implements Target {

    private static final long serialVersionUID = -583488154227852034L;

    @ExcludeHashCodeEquals
    private final Observer observer;

    private final Location location;

    public AbstractTarget(Observer observer, int id, String name, Location location) {
	super(observer, id, name);
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
    public void notifyObserver() {
	this.observer.registerTargetObject(this);
    }
    
    @Override
    public int hashCode() {
    	return getId();
    }
}
