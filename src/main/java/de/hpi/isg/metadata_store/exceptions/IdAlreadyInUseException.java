package de.hpi.isg.metadata_store.exceptions;

import de.hpi.isg.metadata_store.domain.MetadataStore;

/**
 * This {@link Exception} is thrown if a certain id is already taken inside one
 * {@link MetadataStore}.
 *
 */
public class IdAlreadyInUseException extends IllegalArgumentException {

    private static final long serialVersionUID = 7299719181439998338L;

    public IdAlreadyInUseException(String cause) {
	super(cause);
    }
}
