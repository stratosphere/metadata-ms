package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashSet;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

public class TargetTest {

    @Test
    public void testColumnHashCodeAndEquals() {
        final MetadataStore store = new DefaultMetadataStore();

        final DefaultLocation iloc = new DefaultLocation();

        final Column column1 = DefaultColumn.buildAndRegister(store, mock(Table.class), "foo", iloc);

        final Column column3 = DefaultColumn.buildAndRegister(store, mock(Table.class), "foo2", iloc);

        final HashSet<Target> set = new HashSet<Target>();
        set.add(column1);
        assertTrue(set.contains(column1));
        assertFalse(set.contains(column3));
    }

    @Test
    public void testSchemaEquals() {
        // setup schema
        final Schema dummySchema = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "PDB",
                new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "dummyTable",
                dummyTableLocation);

        DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "dummyColumn",
                new DefaultLocation());

        // setup schema
        final Schema dummySchema2 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "PDB",
                new DefaultLocation());

        final DefaultLocation dummyTableLocation2 = new DefaultLocation();

        DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "dummyTable",
                dummyTableLocation2);

        DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "dummyColumn",
                new DefaultLocation());

        assertEquals(dummySchema, dummySchema2);

    }

    @Test
    public void testSchemaGetTable() {
        final DefaultLocation loc = new DefaultLocation();

        final Table table1 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "foo", loc);

        final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", loc).addTable(table1);

        assertEquals(schema1.getTableByName("foo"), table1);
    }

    @Test(expected = NameAmbigousException.class)
    public void testSchemaGetTableForAmbigousNameFails() {
        final MetadataStore ms = new DefaultMetadataStore();

        final Schema schema1 = ms.addSchema("foo", mock(Location.class));

        schema1.addTable(ms, "foo", mock(Location.class));
        schema1.addTable(ms, "foo", mock(Location.class));

        schema1.getTableByName("foo");

    }

    @Test
    public void testSchemaGetTableForUnknownTableReturnNull() {
        final DefaultLocation loc = new DefaultLocation();

        final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", loc);

        assertEquals(schema1.getTableByName("foo"), null);
    }

    @Test
    public void testSchemaFindColumn() {

        final MetadataStore ms = mock(MetadataStore.class);
        final Schema schema1 = DefaultSchema.buildAndRegister(ms, "foo", new DefaultLocation());
        final Table table1 = DefaultTable.buildAndRegister(ms, schema1, "table1", new DefaultLocation());
        final Column column1 = DefaultColumn.buildAndRegister(ms, table1, "column1", new DefaultLocation());

        schema1.addTable(table1);
        table1.addColumn(column1);

        assertEquals(column1, schema1.findColumn(column1.getId()));
    }

    @Test
    public void testSchemaFindColumnForUnknownColumnIdReturnNull() {

        final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", mock(Location.class));

        final Column column1 = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "column1",
                new DefaultLocation());

        assertEquals(null, schema1.findColumn(column1.getId()));
    }

    @Test
    public void testSchemaHashCodeAndEquals() {
        final DefaultLocation loc = new DefaultLocation();

        final Column column1 = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "foo",
                new DefaultLocation());

        final Table table1 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "foo", loc)
                .addColumn(column1);

        final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", loc).addTable(table1);
        final Schema schema2 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", loc).addTable(table1);

        final Schema schema3 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo2", loc);

        assertEquals(schema1, schema2);

        assertEquals(schema1.hashCode(), schema2.hashCode());
        assertFalse(schema1.equals(schema3));
    }

    @Test
    public void testTableHashCodeAndEquals() {

        final DefaultLocation loc = new DefaultLocation();

        final Column column1 = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), 1, "foo",
                new DefaultLocation());

        final Table table1 = DefaultTable
                .buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 2, "foo", loc).addColumn(column1);

        final Table table2 = DefaultTable
                .buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 2, "foo", loc).addColumn(column1);

        final Table table3 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 3, "foo2",
                loc);

        assertEquals(table1, table2);

        assertEquals(table1.hashCode(), table2.hashCode());
        assertFalse(table1.equals(table3));
    }
}
