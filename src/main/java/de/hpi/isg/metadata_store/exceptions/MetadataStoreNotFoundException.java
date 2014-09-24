package de.hpi.isg.metadata_store.exceptions;

public class MetadataStoreNotFoundException extends IllegalStateException {

    private static final long serialVersionUID = -2006107236419278795L;

    public MetadataStoreNotFoundException(Exception e) {
	super(e);
    }
}
