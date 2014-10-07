package de.hpi.isg.metadata_store.exceptions;

import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Schema;

/**
 * This exception is thrown if the {@link MetadataStore} is queried for a not
 * existing {@link Schema}.
 *
 */
public class NameAmbigousException extends IllegalArgumentException {

    /**
     *
     */
    private static final long serialVersionUID = 8356025812350201360L;

    public NameAmbigousException(String name) {
	super("Name is ambigous: " + name);
    }

}
