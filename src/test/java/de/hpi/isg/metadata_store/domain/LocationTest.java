package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;

public class LocationTest {

    @Test
    public void testDefaultLocation() {

        final DefaultLocation location1 = new DefaultLocation();
        location1.getProperties().put(Location.PATH, "hdfs//123");

        final DefaultLocation location2 = new DefaultLocation();
        location2.getProperties().put(Location.PATH, "hdfs//123");

        final DefaultLocation location3 = new DefaultLocation();
        location3.getProperties().put(Location.PATH, "hdfs//321");

        assertEquals(location1, location2);
        assertFalse(location1.equals(location3));
        assertFalse(location2.equals(location3));
    }

    @Test
    public void testDefaultLocationGetIfExists() {

        final DefaultLocation location1 = new DefaultLocation();
        location1.set(Location.PATH, "hdfs//123");

        assertEquals(location1.getIfExists(Location.PATH), "hdfs//123");
        assertEquals(location1.get(Location.PATH), "hdfs//123");
    }

    @Test
    public void testDefaultLocationCreateForFile() {

        final DefaultLocation location1 = DefaultLocation.createForFile("hdfs//123");
        assertEquals(location1.getIfExists(Location.PATH), "hdfs//123");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefaultLocationGetIfExistsFails() {
        final DefaultLocation location1 = new DefaultLocation();

        location1.getIfExists(Location.PATH);
    }

}
