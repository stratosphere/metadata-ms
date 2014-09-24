package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;

public class LocationTest {

    @Test
    public void testHDFSLocation() {

	HDFSLocation location1 = new HDFSLocation("foo");
	HDFSLocation location2 = new HDFSLocation("foo");

	assertEquals(location1, location2);
    }

    @Test
    public void testIndexedLocation() {

	HDFSLocation location1 = new HDFSLocation("foo");
	IndexedLocation iLocation1 = new IndexedLocation(1, location1);
	IndexedLocation iLocation2 = new IndexedLocation(1, location1);

	assertEquals(iLocation1, iLocation2);
    }

}
