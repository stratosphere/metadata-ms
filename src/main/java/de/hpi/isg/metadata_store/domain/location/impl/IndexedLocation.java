package de.hpi.isg.metadata_store.domain.location.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * A {@link Location} representing a {@link Column} within a file by an index.
 * Therefore it holds a reference to its parent {@link HDFSLocation}.
 * 
 * @author fabian
 *
 */

public class IndexedLocation extends AbstractHashCodeAndEquals implements Location {

    private static final long serialVersionUID = -4987116057109358698L;

    private long index;

    private Location parentLocation;
    
    private int offset;
    
	public IndexedLocation(long index, Location parentLocation) {
		this(index, parentLocation, 1);
	}

	public IndexedLocation(long index, Location parentLocation, int offset) {
	super();
	this.index = index;
	this.parentLocation = parentLocation;
	this.offset = offset;
    }

    public long getIndex() {
	return this.index;
    }

    public Location getParentLocation() {
	return this.parentLocation;
    }

    public void setIndex(long index) {
	this.index = index;
    }

    public void setParentLocation(HDFSLocation parentLocation) {
	this.parentLocation = parentLocation;
    }
    
    public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

    @Override
    public String toString() {
	return "IndexedLocation [index=" + this.index + ", parentLocation=" + this.parentLocation
		+ ", getParentLocation()=" + this.getParentLocation() + ", getIndex()=" + this.getIndex() + "]";
    }
}
