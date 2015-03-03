package de.hpi.isg.mdms.domain.common;

import de.hpi.isg.mdms.domain.MetadataStore;

/**
 * Everything that shall be named for easier understandability within a {@link MetadataStore} is {@link Named}. Names do
 * not have to be unique within on {@link MetadataStore}.
 *
 */
public interface Named {
    /**
     * Returns the name of a {@link Named} object.
     * 
     * @return the name
     */
    public String getName();
}
