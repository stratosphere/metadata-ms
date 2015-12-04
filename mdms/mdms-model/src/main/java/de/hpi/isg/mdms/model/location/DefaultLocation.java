package de.hpi.isg.mdms.model.location;

import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link de.hpi.isg.mdms.model.location.Location} representing a HDFS location.
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
        return this.properties.get(propertyKey);
    }

    @Override
    public String get(String propertyKey) {
        if (!this.properties.containsKey(propertyKey)) {
            throw new IllegalArgumentException(
                    String.format("No property associated with %s in %s.", propertyKey, this));
        }
        return getIfExists(propertyKey);
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
