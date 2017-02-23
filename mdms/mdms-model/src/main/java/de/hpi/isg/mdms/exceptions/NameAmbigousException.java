package de.hpi.isg.mdms.exceptions;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;

/**
 * This exception is thrown if the {@link MetadataStore} is queried for a not existing {@link Schema}.
 *
 */
public class NameAmbigousException extends MetadataStoreException {

    public NameAmbigousException(final String name) {
        super("Name is ambigous: " + name);
    }

}
