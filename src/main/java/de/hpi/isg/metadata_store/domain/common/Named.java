package de.hpi.isg.metadata_store.domain.common;

import de.hpi.isg.metadata_store.domain.MetadataStore;

/**
 * Everything that shall be named for easier understandability within a
 * {@link MetadataStore} is {@link Named}. Names do not have to be unique within
 * on {@link MetadataStore}.
 *
 */
public interface Named {
    public String getName();
}
