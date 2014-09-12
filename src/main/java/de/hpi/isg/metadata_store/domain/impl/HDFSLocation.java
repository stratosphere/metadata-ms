package de.hpi.isg.metadata_store.domain.impl;

import de.hpi.isg.metadata_store.domain.ILocation;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

public class HDFSLocation extends AbstractHashCodeAndEquals implements ILocation {

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
}
