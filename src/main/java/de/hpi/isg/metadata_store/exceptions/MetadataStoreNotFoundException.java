package de.hpi.isg.metadata_store.exceptions;

import de.hpi.isg.metadata_store.domain.MetadataStore;

/**
 * This {@link Exception} is thrown if a certain {@link MetadataStore} can not
 * be found for a given location.
 *
 */
public class MetadataStoreNotFoundException extends IllegalStateException {

    private static final long serialVersionUID = -2006107236419278795L;

    public MetadataStoreNotFoundException(Exception e) {
	super(e);
    }
}
