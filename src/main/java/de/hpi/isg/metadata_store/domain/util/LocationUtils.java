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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;

/**
 * Contains utility methods for the work with {@link Location} objects.
 * 
 * @author Sebastian Kruse
 */
public class LocationUtils {

    private static final Logger LOGGER = Logger.getAnonymousLogger();

    private LocationUtils() {
    }

    /**
     * Creates a new {@link Location} based on the given class name and properties. If no {@link Location} could be
     * created for the given class name, a {@link DefaultLocation} will be returned instead.
     * 
     * @param className is the class name of a {@link Location} subtype
     * @param properties are the properties to take on by the location to be created
     * @return a location with the given properties
     */
    public static Location createLocation(String className, Map<String, String> properties) {
        Location location = null;
        try {
            Class<? extends Location> locationClass = Class.forName(className).asSubclass(Location.class);
            location = locationClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.log(Level.WARNING,
                    String.format("Could not instantiate location of class %s -- using DefaultLocation.", className), e);
            location = new DefaultLocation();
        }
        location.getProperties().putAll(properties);

        return location;
    }

}
