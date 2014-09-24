package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;

public class TargetTest {

    @Test
    public void testColumnHashCodeAndEquals() {
	MetadataStore store1 = new DefaultMetadataStore();

	HDFSLocation loc = new HDFSLocation("foobar");

	DefaultColumn column1 = DefaultColumn.buildAndRegister(store1, 1, "foo", loc);

	DefaultColumn column2 = DefaultColumn.buildAndRegister(store1, 1, "foo", loc);

	DefaultColumn column3 = DefaultColumn.buildAndRegister(store1, 2, "foo2", loc);

	assertEquals(column1, column2);

	assertEquals(column1.hashCode(), column2.hashCode());

	HashSet<Target> set = new HashSet<Target>();
	set.add(column1);
	assertTrue(set.contains(column1));
	assertTrue(set.contains((Target) column2));
	assertFalse(set.contains((Target) column3));
    }

    @Test
    public void testTableHashCodeAndEquals() {
	MetadataStore store1 = new DefaultMetadataStore();

	HDFSLocation loc = new HDFSLocation("foobar");

	DefaultColumn column1 = DefaultColumn.buildAndRegister(store1, 1, "foo", loc);

	Table table1 = DefaultTable.buildAndRegister(store1, 1, "foo", loc).addColumn(column1);

	Table table2 = DefaultTable.buildAndRegister(store1, 1, "foo", loc).addColumn(column1);

	Table table3 = DefaultTable.buildAndRegister(store1, 2, "foo2", loc);

	assertEquals(table1, table2);

	assertEquals(table1.hashCode(), table2.hashCode());
	assertFalse(table1.equals(table3));
    }

    @Test
    public void testSchemaHashCodeAndEquals() {
	MetadataStore store1 = new DefaultMetadataStore();

	HDFSLocation loc = new HDFSLocation("foobar");

	DefaultColumn column1 = DefaultColumn.buildAndRegister(store1, 1, "foo", loc);

	Table table1 = DefaultTable.buildAndRegister(store1, 1, "foo", loc).addColumn(column1);

	Schema schema1 = DefaultSchema.buildAndRegister(store1, 1, "foo", loc).addTable(table1);
	Schema schema2 = DefaultSchema.buildAndRegister(store1, 1, "foo", loc).addTable(table1);

	Schema schema3 = DefaultSchema.buildAndRegister(store1, 2, "foo2", loc);

	assertEquals(schema1, schema2);

	assertEquals(schema1.hashCode(), schema2.hashCode());
	assertFalse(schema1.equals(schema3));
    }

    @Test
    public void testSchemaEquals() {
	MetadataStore store1 = new DefaultMetadataStore();
	// setup schema
	Schema dummySchema = DefaultSchema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));

	HDFSLocation dummyTableLocation = new HDFSLocation("hdfs://foobar/dummyTable.csv");

	Table dummyTable = DefaultTable.buildAndRegister(store1, 3, "dummyTable", dummyTableLocation);

	Column dummyColumn = DefaultColumn.buildAndRegister(store1, 4, "dummyColumn", new IndexedLocation(0,
		dummyTableLocation));

	store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn)));

	// setup schema
	Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, 2, "PDB", new HDFSLocation("hdfs://foobar"));

	HDFSLocation dummyTableLocation2 = new HDFSLocation("hdfs://foobar/dummyTable.csv");

	Table dummyTable2 = DefaultTable.buildAndRegister(store1, 3, "dummyTable", dummyTableLocation2);

	Column dummyColumn2 = DefaultColumn.buildAndRegister(store1, 4, "dummyColumn", new IndexedLocation(0,
		dummyTableLocation2));

	store1.getSchemas().add(dummySchema2.addTable(dummyTable2.addColumn(dummyColumn2)));

	assertEquals(dummySchema, dummySchema2);

    }
}
