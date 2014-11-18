package de.hpi.isg.metadata_store.domain.location.impl;

import java.util.HashMap;
import java.util.Map;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;

/**
 * A {@link Location} representing a HDFS location.
 *
 *
 */

public class DefaultLocation extends AbstractHashCodeAndEquals implements Location {

    private static final long serialVersionUID = 4906351571223005639L;

    private Map<String, String> properties;

    public DefaultLocation() {
        this.properties = new HashMap<>();
    }

    @Override
    public String toString() {
        return "DefaultLocation [properties=" + properties + "]";
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }
}
