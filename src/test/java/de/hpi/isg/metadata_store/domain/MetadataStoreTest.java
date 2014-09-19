package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.impl.MetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.IColumn;
import de.hpi.isg.metadata_store.domain.targets.ISchema;
import de.hpi.isg.metadata_store.domain.targets.ITable;
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
		//setup store
		IMetadataStore store1 = new MetadataStore(2, "test");
		//setup schema
		ISchema dummySchema = Schema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));
		
		HDFSLocation dummyTableLocation =  new HDFSLocation("hdfs://foobar/dummyTable.csv");
		
		ITable dummyTable = Table.buildAndRegister(store1,3, "dummyTable", dummyTableLocation);
		
		IColumn dummyColumn = Column.buildAndRegister(store1,4, "dummyColumn", new IndexedLocation(0, dummyTableLocation));
		
		IConstraint dummyContraint = new TypeConstraint(5, "dummyTypeConstraint", new SingleTargetReference(dummyColumn));

		store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn)));
		
		store1.addConstraint(dummyContraint);
		
		try {
			MetadataStore.saveMetadataStore(dir, store1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//retrieve store
		IMetadataStore store2 = null;
		
		try {
			store2 = MetadataStore.getMetadataStoreForId(dir, 2);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		assertEquals(store1, store2);
		
		ISchema schema = store2.getSchemas().iterator().next();
		
		assertEquals(dummySchema, schema);
	}

	
	@Test
	public void testStoringOfFilledMetadataStore2() {
		//setup store
		IMetadataStore store1 = new MetadataStore(3, "test");
		//setup schema
		ISchema dummySchema = Schema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));
		store1.getSchemas().add(dummySchema);
		
		try {
			MetadataStore.saveMetadataStore(dir, store1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//retrieve store
		IMetadataStore store2 = null;
		
		try {
			store2 = MetadataStore.getMetadataStoreForId(dir, 3);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		assertEquals(store1, store2);
		
		ISchema schema = store2.getSchemas().iterator().next();
		
		assertEquals(dummySchema, schema);
	}
	
	@Test
	public void testStoringOfFilledMetadataStore3() {
		//setup store
		IMetadataStore store1 = new MetadataStore(4, "test");
		//setup schema
		ISchema dummySchema1 = Schema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar")).addTable(Table.buildAndRegister(store1, 45, "foo", null));
		store1.getSchemas().add(dummySchema1);
		
		try {
			MetadataStore.saveMetadataStore(dir, store1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//retrieve store
		IMetadataStore store2 = null;
		
		try {
			store2 = MetadataStore.getMetadataStoreForId(dir, 4);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals(store1, store2);
	}
}
