package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;
import de.hpi.isg.metadata_store.exceptions.MetadataStoreNotFoundException;

public class MetadataStoreTest {

    private final File dir = new File("test/");

    @Before
    public void setUp() {
	dir.mkdir();
	try {
	    FileUtils.cleanDirectory(dir);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    @After
    public void tearDown() {
	try {
	    FileUtils.cleanDirectory(dir);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    @Test
    public void testStoringOfEmptyMetadataStore() {
	File file = new File(dir, "emptyStore.ms");
	MetadataStore store1 = new DefaultMetadataStore();
	try {
	    DefaultMetadataStore.saveMetadataStore(file, store1);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	MetadataStore store2 = null;
	try {
	    store2 = DefaultMetadataStore.getMetadataStore(file);
	} catch (MetadataStoreNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	assertEquals(store1, store2);
    }

    @Test
    public void testGetOrCreateOfExisting() {
	File file = new File(dir, "filledStore.ms");
	// setup store
	MetadataStore store1 = new DefaultMetadataStore();
	// setup schema
	Schema dummySchema = DefaultSchema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));

	HDFSLocation dummyTableLocation = new HDFSLocation("hdfs://foobar/dummyTable.csv");

	Table dummyTable = DefaultTable.buildAndRegister(store1, 3, "dummyTable", dummyTableLocation);

	Column dummyColumn = DefaultColumn.buildAndRegister(store1, 4, "dummyColumn", new IndexedLocation(0,
		dummyTableLocation));

	Constraint dummyContraint = new TypeConstraint(5, "dummyTypeConstraint", new SingleTargetReference(dummyColumn));

	store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn)));

	store1.addConstraint(dummyContraint);

	try {
	    DefaultMetadataStore.saveMetadataStore(file, store1);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	MetadataStore store2 = null;
	try {
	    store2 = DefaultMetadataStore.getOrCreateAndSaveMetadataStore(file);
	} catch (MetadataStoreNotFoundException | IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	assertEquals(store1, store2);
    }

    @Test
    public void testGetOrCreateOfNotExisting() {
	File file = new File(dir, "notExisting.ms");

	MetadataStore store2 = null;
	try {
	    store2 = DefaultMetadataStore.getOrCreateAndSaveMetadataStore(file);
	} catch (MetadataStoreNotFoundException | IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	assertEquals(store2, new DefaultMetadataStore());
    }

    @Test(expected = MetadataStoreNotFoundException.class)
    public void testGetMetaDataStoreOnNotExistingFails() {
	File file = new File(dir, "nooooootExisting.ms");

	@SuppressWarnings("unused")
	MetadataStore store2;

	store2 = DefaultMetadataStore.getMetadataStore(file);
    }

    @Test
    public void testStoringOfFilledMetadataStore() {
	File file = new File(dir, "filledStore.ms");
	// setup store
	MetadataStore store1 = new DefaultMetadataStore();
	// setup schema
	Schema dummySchema = DefaultSchema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));

	HDFSLocation dummyTableLocation = new HDFSLocation("hdfs://foobar/dummyTable.csv");

	Table dummyTable = DefaultTable.buildAndRegister(store1, 3, "dummyTable", dummyTableLocation);

	Column dummyColumn = DefaultColumn.buildAndRegister(store1, 4, "dummyColumn", new IndexedLocation(0,
		dummyTableLocation));

	Constraint dummyContraint = new TypeConstraint(5, "dummyTypeConstraint", new SingleTargetReference(dummyColumn));

	store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn)));

	store1.addConstraint(dummyContraint);

	try {
	    DefaultMetadataStore.saveMetadataStore(file, store1);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	// retrieve store
	MetadataStore store2 = null;

	try {
	    store2 = DefaultMetadataStore.getMetadataStore(file);
	} catch (MetadataStoreNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	assertEquals(dummySchema, store2.getSchemas().iterator().next());

	assertEquals(store1, store2);
    }

    @Test
    public void testStoringOfFilledMetadataStore2() {
	File file = new File(dir, "filledStore.ms");
	// setup store
	MetadataStore store1 = new DefaultMetadataStore();
	// setup schema
	Schema dummySchema = DefaultSchema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));
	store1.getSchemas().add(dummySchema);

	try {
	    DefaultMetadataStore.saveMetadataStore(file, store1);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	// retrieve store
	MetadataStore store2 = null;

	try {
	    store2 = DefaultMetadataStore.getMetadataStore(file);
	} catch (MetadataStoreNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	assertEquals(store1, store2);

	Schema schema = store2.getSchemas().iterator().next();

	assertEquals(dummySchema, schema);
    }

    @Test
    public void testStoringOfFilledMetadataStore3() {
	File file = new File(dir, "filledStore.ms");
	// setup store
	MetadataStore store1 = new DefaultMetadataStore();
	// setup schema
	Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"))
		.addTable(DefaultTable.buildAndRegister(store1, 45, "foo", null));
	store1.getSchemas().add(dummySchema1);

	try {
	    DefaultMetadataStore.saveMetadataStore(file, store1);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	// retrieve store
	MetadataStore store2 = null;

	try {
	    store2 = DefaultMetadataStore.getMetadataStore(file);
	} catch (MetadataStoreNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	Collection<Target> allTargets1 = store1.getAllTargets();
	Collection<Target> allTargets2 = store2.getAllTargets();
	assertTrue(allTargets1.contains(dummySchema1));
	assertTrue(allTargets2.contains(dummySchema1));

	assertEquals(store1, store2);
    }
}
