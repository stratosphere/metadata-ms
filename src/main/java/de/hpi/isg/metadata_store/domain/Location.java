package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;

/**
 * A {@link Location} represents the physical location of {@link Target}s.
 *
 */
public interface Location extends Serializable {

    public String getPath();

}
