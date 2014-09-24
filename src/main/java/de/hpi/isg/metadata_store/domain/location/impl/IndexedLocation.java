package de.hpi.isg.metadata_store.domain.location.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

public class IndexedLocation extends AbstractHashCodeAndEquals implements Location {

    private static final long serialVersionUID = -4987116057109358698L;

    private long index;

    private HDFSLocation parentLocation;

    public IndexedLocation(long index, HDFSLocation parentLocation) {
	super();
	this.index = index;
	this.parentLocation = parentLocation;
    }

    public HDFSLocation getParentLocation() {
	return parentLocation;
    }

    public void setParentLocation(HDFSLocation parentLocation) {
	this.parentLocation = parentLocation;
    }

    public long getIndex() {
	return index;
    }

    public void setIndex(long index) {
	this.index = index;
    }

    @Override
    public String toString() {
	return "IndexedLocation [index=" + index + ", parentLocation=" + parentLocation + ", getParentLocation()="
		+ getParentLocation() + ", getIndex()=" + getIndex() + "]";
    }
}
