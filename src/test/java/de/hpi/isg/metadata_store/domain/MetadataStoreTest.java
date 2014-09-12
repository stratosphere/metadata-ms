package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.impl.MetadataStore;
import de.hpi.isg.metadata_store.domain.targets.ISchema;
import de.hpi.isg.metadata_store.domain.targets.impl.Column;
import de.hpi.isg.metadata_store.domain.targets.impl.Schema;
import de.hpi.isg.metadata_store.domain.targets.impl.Table;

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
		IMetadataStore store1 = new MetadataStore(1, "test");
		try {
			MetadataStore.saveMetadataStore(dir, store1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IMetadataStore store2 = null;
		try {
			store2 = MetadataStore.getMetadataStoreForId(dir, 1);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(store1, store2);
	}

	@Test
	public void testStoringOfFilledMetadataStore() {
		IMetadataStore store1 = new MetadataStore(2, "test");
		store1.getSchemas().add(
				new Schema(2, "PDB", new HDFSLocation("hdfs://foobar"))
						.addTable(new Table(3, "gene", new HDFSLocation(
								"hdfs://bla")).addColumn(new Column(4, "foo",
								new IndexedLocation(0, new HDFSLocation(
										"hdfs://bla"))))));
		try {
			MetadataStore.saveMetadataStore(dir, store1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IMetadataStore store2 = null;
		try {
			store2 = MetadataStore.getMetadataStoreForId(dir, 2);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals(store1, store2);
		
		ISchema schema = store2.getSchemas().iterator().next();
		
		
		assertEquals(new Schema(2, "PDB", new HDFSLocation("hdfs://foobar"))
						.addTable(new Table(3, "gene", new HDFSLocation(
								"hdfs://bla")).addColumn(new Column(4, "foo",
								new IndexedLocation(0, new HDFSLocation(
										"hdfs://bla"))))), schema);
	}

}
