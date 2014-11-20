package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Map;

/**
 * A {@link Location} represents the physical location of {@link Target}s.
 *
 */
public interface Location extends Serializable {

    String INDEX = "INDEX";
    String PATH = "PATH";

    public Map<String, String> getProperties();

}
