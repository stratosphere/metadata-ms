package de.hpi.isg.mdms.exceptions;

import de.hpi.isg.mdms.domain.MetadataStore;

/**
 * This {@link Exception} is thrown if a certain id is already taken inside one {@link MetadataStore}.
 *
 */
public class IdAlreadyInUseException extends IllegalArgumentException {

    private static final long serialVersionUID = 7299719181439998338L;

    public IdAlreadyInUseException(final String cause) {
        super(cause);
    }
}
