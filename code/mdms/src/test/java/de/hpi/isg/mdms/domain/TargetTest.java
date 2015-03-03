package de.hpi.isg.mdms.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashSet;

import org.junit.Test;

import de.hpi.isg.mdms.domain.impl.DefaultMetadataStore;
import de.hpi.isg.mdms.domain.location.impl.DefaultLocation;
import de.hpi.isg.mdms.domain.targets.Column;
import de.hpi.isg.mdms.domain.targets.Schema;
import de.hpi.isg.mdms.domain.targets.Table;
import de.hpi.isg.mdms.domain.targets.impl.DefaultColumn;
import de.hpi.isg.mdms.domain.targets.impl.DefaultSchema;
import de.hpi.isg.mdms.domain.targets.impl.DefaultTable;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;

public class TargetTest {

    @Test
    public void testColumnHashCodeAndEquals() {
        final MetadataStore store = new DefaultMetadataStore();

        final DefaultLocation iloc = new DefaultLocation();

        final Column column1 = DefaultColumn.buildAndRegister(store, mock(Table.class), "foo", null, iloc);

        final Column column3 = DefaultColumn.buildAndRegister(store, mock(Table.class), "foo2", null, iloc);

        final HashSet<Target> set = new HashSet<Target>();
        set.add(column1);
        assertTrue(set.contains(column1));
        assertFalse(set.contains(column3));
    }

    @Test
    public void testSchemaEquals() {
        // setup schema
        final Schema dummySchema = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "PDB", null,
                new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "dummyTable", null,
                dummyTableLocation);

        DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "dummyColumn", null,
                new DefaultLocation());

        // setup schema
        final Schema dummySchema2 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "PDB", null,
                new DefaultLocation());

        final DefaultLocation dummyTableLocation2 = new DefaultLocation();

        DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "dummyTable", null,
                dummyTableLocation2);

        DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "dummyColumn", null,
                new DefaultLocation());

        assertEquals(dummySchema, dummySchema2);

    }

    @Test
    public void testSchemaGetTable() {
        MetadataStore store = new DefaultMetadataStore();
        final DefaultLocation loc = new DefaultLocation();

        final Schema schema1 = store.addSchema("bar", "", loc);
        Table table1 = schema1.addTable(store, "foo", "", loc);

        assertEquals(schema1.getTableByName("foo"), table1);
    }

    @Test(expected = NameAmbigousException.class)
    public void testSchemaGetTableForAmbigousNameFails() {
        final MetadataStore ms = new DefaultMetadataStore();

        final Schema schema1 = ms.addSchema("foo", null, mock(Location.class));

        schema1.addTable(ms, "foo", null, mock(Location.class));
        schema1.addTable(ms, "foo", null, mock(Location.class));

        schema1.getTableByName("foo");

    }

    @Test
    public void testSchemaGetTableForUnknownTableReturnNull() {
        final DefaultLocation loc = new DefaultLocation();

        final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", null, loc);

        assertEquals(schema1.getTableByName("foo"), null);
    }

    @Test
    public void testSchemaFindColumn() {

        final MetadataStore ms = new DefaultMetadataStore();
        final Schema schema1 = ms.addSchema("foo", "", new DefaultLocation());

        final Table table1 = schema1.addTable(ms, "table1", "", new DefaultLocation());
        final Column column1 = table1.addColumn(ms, "column1", "", 1);

        assertEquals(column1, schema1.findColumn(column1.getId()));
    }

    @Test
    public void testSchemaFindColumnForUnknownColumnIdReturnNull() {

        final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", null,
                mock(Location.class));

        final Column column1 = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "column1",
                null,
                new DefaultLocation());

        assertEquals(null, schema1.findColumn(column1.getId()));
    }

    @Test
    public void testSchemaHashCodeAndEquals() {
        MetadataStore store1 = new DefaultMetadataStore();
        MetadataStore store2 = new DefaultMetadataStore();
        final DefaultLocation loc = new DefaultLocation();

        final Schema schema1 = store1.addSchema("foo", "", loc);
        schema1.addTable(store1, "foo", "", loc);
        final Schema schema2 = store2.addSchema("foo", "", loc);
        schema2.addTable(store2, "foo", "", loc);

        final Schema schema3 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo2", null, loc);

        assertEquals(schema1, schema2);

        assertEquals(schema1.hashCode(), schema2.hashCode());
        assertFalse(schema1.equals(schema3));
    }

    @Test
    public void testTableHashCodeAndEquals() {

        final MetadataStore store1 = new DefaultMetadataStore();
        final MetadataStore store2 = new DefaultMetadataStore();
        final DefaultLocation loc = new DefaultLocation();

        Table table1 = store1.addSchema("", "", loc).addTable(store1, "foo", "bar", loc);
        table1.addColumn(store1, "col", "des", 0);

        Table table2 = store2.addSchema("", "", loc).addTable(store2, "foo", "bar", loc);
        table1.addColumn(store2, "col", "des", 0);

        assertEquals(table1, table2);

        assertEquals(table1.hashCode(), table2.hashCode());
    }
}
