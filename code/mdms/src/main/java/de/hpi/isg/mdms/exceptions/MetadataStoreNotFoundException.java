package de.hpi.isg.mdms.exceptions;

import de.hpi.isg.mdms.domain.MetadataStore;

/**
 * This {@link Exception} is thrown if a certain {@link MetadataStore} can not be found for a given location.
 *
 */
public class MetadataStoreNotFoundException extends IllegalStateException {

    private static final long serialVersionUID = -2006107236419278795L;

    public MetadataStoreNotFoundException(final Exception e) {
        super(e);
    }
}
