package de.hpi.isg.metadata_store.domain.common;

import de.hpi.isg.metadata_store.domain.MetadataStore;

/**
 * Everything that shall be unambiguously identified within a {@link MetadataStore} by a numeric id is
 * {@link Identifiable}.
 *
 */
public interface Identifiable {
    public int getId();
}
