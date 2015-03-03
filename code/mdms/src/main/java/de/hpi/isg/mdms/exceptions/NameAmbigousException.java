package de.hpi.isg.mdms.exceptions;

import de.hpi.isg.mdms.domain.MetadataStore;
import de.hpi.isg.mdms.domain.targets.Schema;

/**
 * This exception is thrown if the {@link MetadataStore} is queried for a not existing {@link Schema}.
 *
 */
public class NameAmbigousException extends IllegalArgumentException {

    /**
     *
     */
    private static final long serialVersionUID = 8356025812350201360L;

    public NameAmbigousException(final String name) {
        super("Name is ambigous: " + name);
    }

}
