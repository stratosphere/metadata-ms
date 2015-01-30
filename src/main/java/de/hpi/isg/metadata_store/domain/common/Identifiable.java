package de.hpi.isg.metadata_store.domain.common;

import de.hpi.isg.metadata_store.domain.MetadataStore;

/**
 * Everything that shall be unambiguously identified within a {@link MetadataStore} by a numeric id is
 * {@link Identifiable}.
 *
 */
public interface Identifiable {
    /**
     * Returns the id of an {@link Identifiable} object.
     * 
     * @return the id
     */
    public int getId();
}
