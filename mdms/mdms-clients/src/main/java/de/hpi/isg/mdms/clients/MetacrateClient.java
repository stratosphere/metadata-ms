package de.hpi.isg.mdms.clients;

import de.hpi.isg.mdms.model.MetadataStore;

/**
 * Implementers can receive a {@link MetadataStore}.
 */
public interface MetacrateClient {

    /**
     * Set this instance up with a {@link MetadataStore}.
     *
     * @param metadataStore the {@link MetadataStore}
     */
    void setMetadataStore(MetadataStore metadataStore);

}
