package de.hpi.isg.metadata_store.domain.location.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * A {@link Location} representing a {@link Column} within a file by an index. Therefore it holds a reference to its
 * parent {@link HDFSLocation}.
 *
 * @author fabian
 *
 */

public class IndexedLocation extends AbstractHashCodeAndEquals implements Location {

    private static final long serialVersionUID = -4987116057109358698L;

    private long index;

    private Location parentLocation;

    public IndexedLocation(final long index, final Location parentLocation) {
        this.index = index;
        this.parentLocation = parentLocation;
    }

    public long getIndex() {
        return this.index;
    }

    public Location getParentLocation() {
        return this.parentLocation;
    }

    public void setIndex(final long index) {
        this.index = index;
    }

    public void setParentLocation(final HDFSLocation parentLocation) {
        this.parentLocation = parentLocation;
    }

    @Override
    public String toString() {
        return "IndexedLocation [index=" + this.index + ", parentLocation=" + this.parentLocation
                + ", getParentLocation()=" + this.getParentLocation() + ", getIndex()=" + this.getIndex() + "]";
    }

    @Override
    public String getPath() {
        return this.parentLocation.getPath();
    }
}
