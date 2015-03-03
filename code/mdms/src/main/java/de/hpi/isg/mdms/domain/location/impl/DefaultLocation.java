package de.hpi.isg.mdms.domain.location.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import de.hpi.isg.mdms.domain.Location;
import de.hpi.isg.mdms.domain.common.impl.AbstractHashCodeAndEquals;

/**
 * A {@link Location} representing a HDFS location.
 * 
 * 
 */

public class DefaultLocation extends AbstractHashCodeAndEquals implements Location {

    
    private static final long serialVersionUID = 4906351571223005639L;

    private Map<String, String> properties;

    public static DefaultLocation createForFile(String path) {
        DefaultLocation location = new DefaultLocation();
        location.set(PATH, path);
        return location;
    }

    public DefaultLocation() {
        this.properties = new HashMap<>();
    }

    @Override
    public void set(String propertyKey, String value) {
        this.properties.put(propertyKey, value);
    }
    
    @Override
    public void delete(String propertyKey) {
    	this.properties.remove(propertyKey);
    }

    @Override
    public String getIfExists(String propertyKey) {
        if (!this.properties.containsKey(propertyKey)) {
            throw new IllegalArgumentException(
                    String.format("No property associated with %s in %s.", propertyKey, this));
        }
        return get(propertyKey);
    }

    @Override
    public String get(String propertyKey) {
        return this.properties.get(propertyKey);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [properties=" + properties + "]";
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }
    
    @Override
    public Collection<String> getAllPropertyKeys() {
        return Arrays.asList(TYPE, INDEX, PATH);
    }
    
    @Override
    public Collection<String> getPropertyKeysForValueCanonicalization() {
        return Arrays.asList(TYPE, INDEX);
    }

}
