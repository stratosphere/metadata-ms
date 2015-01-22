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
package de.hpi.isg.metadata_store.domain.util;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;

/**
 * Contains utility methods for the work with {@link Location} objects.
 * 
 * @author Sebastian Kruse
 */
public class LocationUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationUtils.class);

    private static final Map<String, String> KNOWN_PROPERTIES = new HashMap<>();

    private static final Map<String, Map<String, String>> CANONICAL_PROPERTY_VALUES = new HashMap<>();

    private static final Map<Integer, Class<? extends Location>> classHashCode2ClassMapping = new HashMap<>();

    static {
        classHashCode2ClassMapping.put(DefaultLocation.class.getName().hashCode(), DefaultLocation.class);
    }

    private LocationUtils() {
    }

    public static void registerLocationType(Class<? extends Location> locationType) {
        classHashCode2ClassMapping.put(locationType.getName().hashCode(), locationType);
    }
    
    /**
     * Registering a property allows to reuse the property key String object in new instances.
     * 
     * @param key
     *        is the key of the property
     */
    public static void registerProperty(String key) {
        synchronized (KNOWN_PROPERTIES) {
            KNOWN_PROPERTIES.put(key, key);
        }
    }

    /**
     * Registering a property here allows to share {@link String} objects for the property values among different
     * {@link Location} objects.
     * 
     * @param key
     *        is the key of the property
     */
    public static void registerPropertyForValueCanoicalization(String key) {
        synchronized (CANONICAL_PROPERTY_VALUES) {
            CANONICAL_PROPERTY_VALUES.put(key, new HashMap<String, String>());
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
    public static Location createLocation(Integer classNameHashCode, Map<String, String> properties) {
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

    private static Class<? extends Location> lookupClassFor(Integer classNameHashCode) {
        Class<? extends Location> clazz = classHashCode2ClassMapping.get(classNameHashCode);
        if (clazz == null) {
            LOGGER.warn(
                    "The given class hash code is not known for any registered location Type, did you register your location type in {}",
                    LocationUtils.class);
            return DefaultLocation.class;
        }
        return clazz;
    }

    public static void setCanonicalProperty(String key, String value, Location location) {
        String knownProperty;
        synchronized (KNOWN_PROPERTIES) {
            knownProperty = KNOWN_PROPERTIES.get(key);
        }
        if (knownProperty != null) {
            key = knownProperty;
        }

        Map<String, String> knownPropertyValues;
        synchronized (CANONICAL_PROPERTY_VALUES) {
            knownPropertyValues = CANONICAL_PROPERTY_VALUES.get(value);
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
        location.getProperties().put(key, value);
    }

}
