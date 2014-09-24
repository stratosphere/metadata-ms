package de.hpi.isg.metadata_store.domain.location.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

public class HDFSLocation extends AbstractHashCodeAndEquals implements Location {

    private String path;

    private static final long serialVersionUID = 4906351571223005639L;

    public HDFSLocation(String path) {
	this.path = path;
    }

    public String getPath() {
	return path;
    }

    public void setPath(String path) {
	this.path = path;
    }

    @Override
    public String toString() {
	return "HDFSLocation [path=" + path + ", getPath()=" + getPath() + "]";
    }
}
