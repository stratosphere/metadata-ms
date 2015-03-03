/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.domain.util;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.mdms.domain.Location;
import de.hpi.isg.mdms.domain.location.impl.DefaultLocation;

/**
 * Contains utility methods for the work with {@link Location} objects.
 * 
 * @author Sebastian Kruse
 */
public class LocationCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationCache.class);

    public static int computeId(Class<? extends Location> locationType) {
        return computeId(locationType.getName());
    }
    
    public static int computeId(String canonicalName) {
        return canonicalName.hashCode();
    }

    private final Map<String, String> knownProperties = new HashMap<>();

    private final Map<String, Map<String, String>> canonicalPropertyValues = new HashMap<>();

    private final Int2ObjectMap<Class<? extends Location>> classHashCode2ClassMapping = new Int2ObjectOpenHashMap<>();

    public LocationCache() {
        classHashCode2ClassMapping.put(DefaultLocation.class.getName().hashCode(), DefaultLocation.class);
    }

    /**
     * Caches the given {@link Location} type.
     * 
     * @param locationType is the {@link Location} type to cache
     * @return whether the locationType has not been cached yet
     */
    public boolean cacheLocationType(Class<? extends Location> locationType) {
        int locationId = computeId(locationType);
        if (!classHashCode2ClassMapping.containsKey(locationId)) {
            classHashCode2ClassMapping.put(locationId, locationType);
            
            // Try to register the properties etc.
            try {
                Location location = locationType.newInstance();
                for (String propertyKey : location.getAllPropertyKeys()) {
                    cachePropertyKey(propertyKey);
                }
                for (String propertyKey : location.getPropertyKeysForValueCanonicalization()) {
                    registerPropertyForValueCanoicalization(propertyKey);
                }
            } catch (InstantiationException | IllegalAccessException e) {
                LOGGER.error("Could not register properties etc.", e);
            }
            
            return true;
        }
        return false;
    }
    
    /**
     * Registering a property allows to reuse the property key String object in new instances.
     * 
     * @param key
     *        is the key of the property
     */
    public void cachePropertyKey(String key) {
        synchronized (knownProperties) {
            knownProperties.put(key, key);
        }
    }

    /**
     * Registering a property here allows to share {@link String} objects for the property values among different
     * {@link Location} objects.
     * 
     * @param key
     *        is the key of the property
     */
    public void registerPropertyForValueCanoicalization(String key) {
        synchronized (canonicalPropertyValues) {
            canonicalPropertyValues.put(key, new HashMap<String, String>());
        }
    }

    /**
     * Creates a new {@link Location} based on the given class name and properties. If no {@link Location} could be
     * created for the given class name, a {@link DefaultLocation} will be returned instead.
     * 
     * @param className
     *        is the class name of a {@link Location} subtype
     * @param properties
     *        are the properties to take on by the location to be created
     * @return a location with the given properties
     */
    public Location createLocation(Integer classNameHashCode, Map<String, String> properties) {
        Location location = null;
        try {
            Class<? extends Location> locationClass = lookupClassFor(classNameHashCode);
            location = locationClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.warn("Could not instantiate location -- using DefaultLocation.", e);
            location = new DefaultLocation();
        }

        for (Map.Entry<String, String> property : properties.entrySet()) {
            String key = property.getKey();
            String value = property.getValue();
            setCanonicalProperty(key, value, location);
        }

        return location;
    }

    private Class<? extends Location> lookupClassFor(Integer classNameHashCode) {
        Class<? extends Location> clazz = classHashCode2ClassMapping.get(classNameHashCode);
        if (clazz == null) {
            LOGGER.warn(
                    "The given class hash code {} is not known for any registered location Type, did you register your location type in {}",
                    classNameHashCode, LocationCache.class);
            return DefaultLocation.class;
        }
        return clazz;
    }

    public void setCanonicalProperty(String key, String value, Location location) {
        String knownProperty;
        synchronized (knownProperties) {
            knownProperty = knownProperties.get(key);
        }
        if (knownProperty != null) {
            key = knownProperty;
        }

        Map<String, String> knownPropertyValues;
        synchronized (canonicalPropertyValues) {
            knownPropertyValues = canonicalPropertyValues.get(value);
        }
        if (knownPropertyValues != null) {
            synchronized (knownPropertyValues) {
                if (!knownPropertyValues.containsKey(knownProperty)) {
                    knownPropertyValues.put(value, value);
                } else {
                    value = knownPropertyValues.get(value);
                }
            }
        }
        location.set(key, value);
    }

}
