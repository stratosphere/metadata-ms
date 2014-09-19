package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.impl.MetadataStore;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.targets.ISchema;
import de.hpi.isg.metadata_store.domain.targets.ITable;
import de.hpi.isg.metadata_store.domain.targets.impl.Column;
import de.hpi.isg.metadata_store.domain.targets.impl.Schema;
import de.hpi.isg.metadata_store.domain.targets.impl.Table;

public class TargetTest {

	@Test
	public void testColumnHashCodeAndEquals() {
		IMetadataStore store1 = new MetadataStore(1, "test");
		
		HDFSLocation loc = new HDFSLocation("foobar");
		
		Column column1 = Column.buildAndRegister(store1, 1, "foo", loc);
		
		Column column2 =  Column.buildAndRegister(store1, 1, "foo", loc);
		
		Column column3 =  Column.buildAndRegister(store1, 2, "foo2", loc);
		
		assertEquals(column1, column2);
		
		assertEquals(column1.hashCode(), column2.hashCode());
		
		HashSet<ITarget> set = new HashSet<ITarget>();
		set.add(column1);
		assertTrue(set.contains(column1));
		assertTrue(set.contains((ITarget)column2));
		assertFalse(set.contains((ITarget)column3));
	}
	
	@Test
	public void testTableHashCodeAndEquals() {
		IMetadataStore store1 = new MetadataStore(1, "test");
		
		HDFSLocation loc = new HDFSLocation("foobar");
		
		Column column1 = Column.buildAndRegister(store1, 1, "foo", loc);
		
		ITable table1 = Table.buildAndRegister(store1, 1, "foo", loc).addColumn(column1);
		
		ITable table2 =  Table.buildAndRegister(store1, 1, "foo", loc).addColumn(column1);
		
		ITable table3 =  Table.buildAndRegister(store1, 2, "foo2", loc);
		
		assertEquals(table1, table2);
		
		assertEquals(table1.hashCode(), table2.hashCode());
		assertFalse(table1.equals(table3));
	}
	
	@Test
	public void testSchemaHashCodeAndEquals() {
		IMetadataStore store1 = new MetadataStore(1, "test");
		
		HDFSLocation loc = new HDFSLocation("foobar");
		
		Column column1 = Column.buildAndRegister(store1, 1, "foo", loc);
		
		ITable table1 = Table.buildAndRegister(store1, 1, "foo", loc).addColumn(column1);
		
		ISchema schema1 =  Schema.buildAndRegister(store1, 1, "foo", loc).addTable(table1);
		ISchema schema2 =  Schema.buildAndRegister(store1, 1, "foo", loc).addTable(table1);
		
		ISchema schema3 =  Schema.buildAndRegister(store1, 2, "foo2", loc);
		
		assertEquals(schema1, schema2);
		
		assertEquals(schema1.hashCode(), schema2.hashCode());
		assertFalse(schema1.equals(schema3));
	}

}
