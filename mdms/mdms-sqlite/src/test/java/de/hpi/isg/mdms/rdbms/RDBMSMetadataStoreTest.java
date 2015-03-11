package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RDBMSMetadataStoreTest {

    private static final int loadFactorForCreateComplexSchemaTest = 7;

    private File testDb;
    private Connection connection;

    @Before
    public void setUp() {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

        // RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
    }

    @After
    public void tearDown() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // this.testDb.delete();
    }

    // not longer useful test
    @Ignore
    @Test
    public void testExistenceOfTables() {
        DatabaseMetaData meta;
        Set<String> tables = new HashSet<String>(Arrays.asList(SQLiteInterface.tableNames));

        try {
            meta = connection.getMetaData();
            ResultSet res = meta.getTables(null, null, null,
                    new String[]{"TABLE"});
            while (res.next()) {
                // assertTrue(tables.remove(res.getString("TABLE_NAME")));
                if (!tables.remove(res.getString("TABLE_NAME").toLowerCase())) {
                    System.out.println("Unexpected target: " + res.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertTrue(tables.isEmpty());
    }

    @Test
    public void testAddingOfSchema() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", "foobar", new DefaultLocation());
        assertTrue(store1.getSchemas().contains(schema1));
    }

    @Test
    public void testGetOrCreateOfExisting() throws Exception {

        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        // final Schema dummySchema = RDBMSSchema.buildAndRegisterAndAdd(store1, "PDB", new DefaultLocation());
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        Column dummyColumn = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation).addColumn(store1,
                "dummyColumn", null, 0);

        final Constraint dummyContraint = NumberedDummyConstraint.buildAndAddToCollection(dummyColumn,
                mock(ConstraintCollection.class), 100);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        constraintCollection.add(dummyContraint);

        store1.flush();

        MetadataStore store2 = RDBMSMetadataStore.load(new SQLiteInterface(connection));

        assertEquals(store1, store2);
    }

    @Test
    public void testCreationOfEmptyRDBMSMetadataStore() throws Exception {
        MetadataStore store2 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        store2.flush();

        assertEquals(store2, RDBMSMetadataStore.load(new SQLiteInterface(connection)));
    }

    @Test
    public void testRetrievingOfSchemaByName() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());

        assertEquals(store1.getSchemaByName("PDB"), dummySchema1);
    }

    @Test
    public void testConstraintCollections() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        Column col1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "foo", null, 1);
        Column col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        final ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema1);

        final Constraint dummyTypeConstraint1 = NumberedDummyConstraint.buildAndAddToCollection(col1,
                dummyConstraintCollection,
                100);

        final Constraint dummyTypeConstraint2 = NumberedDummyConstraint.buildAndAddToCollection(col2,
                dummyConstraintCollection,
                200);

        assertTrue(store1.getConstraintCollections().contains(dummyConstraintCollection));
        ConstraintCollection constraintCollection = store1.getConstraintCollections().iterator().next();
        assertTrue(constraintCollection.getConstraints().contains(dummyTypeConstraint1));
        assertTrue(constraintCollection.getConstraints().contains(dummyTypeConstraint2));
    }

    @Test
    public void testUnregisteredLocationTypeIsDeserializedAsdefaultLocation() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        System.out.println("The following error is desired...");
        store1.addSchema("Foobar", "description", new Location() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void set(String propertyKey, String value) {
            }

            @Override
            public Map<String, String> getProperties() {
                return new HashMap<>();
            }

            @Override
            public String getIfExists(String propertyKey) {
                return null;
            }

            @Override
            public String get(String propertyKey) {
                return null;
            }

            @Override
            public void delete(String propertyKey) {
            }

            @Override
            public Collection<String> getAllPropertyKeys() {
                return Collections.emptyList();
            }

            @Override
            public Collection<String> getPropertyKeysForValueCanonicalization() {
                return Collections.emptyList();
            }

        });
        assertEquals(DefaultLocation.class, store1.getSchemaByName("Foobar").getLocation().getClass());
        System.out.println("The last error was desired...");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSchemasAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        store1.getSchemas().add(mock(Schema.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTablesAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        store1.addSchema("foo", null, new DefaultLocation()).getTables().add(mock(Table.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetColumnsAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        store1.addSchema("foo", null, new DefaultLocation()).addTable(store1, "bar", null, new DefaultLocation())
                .getColumns()
                .add(mock(Column.class));
    }

    @Test
    public void testRetrievingOfSchemaByNameWithUnknownNameReturnsNull() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema

        assertEquals(store1.getSchemaByName("PDB"), null);
    }

    @Test
    public void testStoringOfFilledMetadataStore() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", null, 1);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null, dummySchema);
        final Constraint dummyContraint = NumberedDummyConstraint.buildAndAddToCollection(
                dummyColumn, mock(ConstraintCollection.class), 100);
        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(new SQLiteInterface(connection));

        assertEquals(dummySchema, store2.getSchemas().iterator().next());

        assertEquals(dummyColumn, store2.getSchemas().iterator().next().getTables().iterator().next().getColumns()
                .iterator().next());

        assertEquals(store1, store2);
    }


    @Test
    public void testStoringOfFilledMetadataStore2() throws Exception {
        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());
        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(new SQLiteInterface(connection));

        assertEquals(store1, store2);

        final Schema schema = store2.getSchemas().iterator().next();

        assertEquals(dummySchema, schema);
    }

    @Test
    public void testGettingOfSchemaByNameAndId() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());

        assertEquals(schema1, store1.getSchemaByName("pdb"));
        assertEquals(schema1, store1.getSchemaById(schema1.getId()));
    }

    @Test
    public void testGettingOfSchemasByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Schema schema2 = store1.addSchema("pdb", null, new DefaultLocation());
        HashSet<Schema> schemas = new HashSet<>();
        schemas.add(schema1);
        schemas.add(schema2);
        assertEquals(schemas, store1.getSchemasByName("pdb"));
    }

    @Test(expected = NameAmbigousException.class)
    public void testGettingOfSchemaByNameFails() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        store1.addSchema("pdb", null, new DefaultLocation());
        store1.addSchema("pdb", null, new DefaultLocation());
        store1.getSchemaByName("pdb");
    }

    @Test
    public void testGettingOfTableByNameAndId() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());

        assertEquals(table1, schema1.getTableByName("foo"));
        assertEquals(table1, schema1.getTableById(table1.getId()));
    }

    @Test
    public void testGettingOfTablesByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Table table2 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        HashSet<Table> tables = new HashSet<>();
        tables.add(table1);
        tables.add(table2);

        assertEquals(tables, schema1.getTablesByName("foo"));
    }

    @Test(expected = NameAmbigousException.class)
    public void testGettingOfTableByNameFails() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        schema1.addTable(store1, "foo", null, new DefaultLocation());
        schema1.addTable(store1, "foo", null, new DefaultLocation());

        schema1.getTableByName("foo");
    }

    @Test
    public void testGettingOfColumnByNameAndId() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", "comment", new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", "ccc", 0);

        assertEquals(column1, table1.getColumnByName("bar"));
        assertEquals(column1, table1.getColumnById(column1.getId()));
    }

    @Test
    public void testGettingOfColumnssByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", null, 0);
        final Collection<Column> columns = new HashSet<>();
        columns.add(column1);

        assertEquals(columns, table1.getColumnsByName("bar"));
    }

    @Test(expected = NameAmbigousException.class)
    public void testGettingOfColumnByNameFails() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        table1.addColumn(store1, "bar", null, 0);
        table1.addColumn(store1, "bar", null, 1);

        table1.getColumnByName("bar");
    }

    @Test
    public void testGettingOfColumnByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo1", null, new DefaultLocation());
        final Table table2 = schema1.addTable(store1, "foo2", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", null, 0);
        table2.addColumn(store1, "bar", null, 1);

        assertEquals(column1, table1.getColumnByName("bar"));
    }

    @Test
    public void testRemovalOfSchema() throws Exception {
        MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        Table table1 = schema1.addTable(store1, "foo1", null, new DefaultLocation());
        Table table2 = schema1.addTable(store1, "foo2", null, new DefaultLocation());

        table1.addColumn(store1, "bar1", null, 0);
        table1.addColumn(store1, "bar2", null, 1);
        table2.addColumn(store1, "bar3", null, 0);

        assertTrue(!store1.getSchemas().isEmpty());

        store1.removeSchema(schema1);

        assertTrue(store1.getSchemas().isEmpty());

        schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        table1 = schema1.addTable(store1, "foo1", null, new DefaultLocation());
        table2 = schema1.addTable(store1, "foo2", null, new DefaultLocation());

        table1.addColumn(store1, "bar", null, 0);
        table1.addColumn(store1, "bar", null, 1);
        table2.addColumn(store1, "bar", null, 0);

        store1.flush();

        store1.removeSchema(schema1);

        assertTrue(store1.getSchemas().isEmpty());
    }

    @Test
    public void testRemovalOfConstraintCollections() throws Exception {
        // setup store
        MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        Table dummyTable1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation());

        Column col1 = dummyTable1.addColumn(store1, "foo", null, 1);
        Column col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema1);

        NumberedDummyConstraint.buildAndAddToCollection(col1, dummyConstraintCollection, 100);

        store1.removeConstraintCollection(dummyConstraintCollection);
        assertTrue(store1.getConstraintCollections().isEmpty());

        // setup store
        store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        dummyTable1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation());

        col1 = dummyTable1.addColumn(store1, "foo", null, 1);
        col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema1);

        NumberedDummyConstraint.buildAndAddToCollection(col1, dummyConstraintCollection, 100);

        /*
         * dummyUCCConstraint = UniqueColumnCombination.buildAndAddToCollection( new
         * UniqueColumnCombination.Reference(new Column[] { col1 }), dummyConstraintCollection);
         */

        store1.removeConstraintCollection(dummyConstraintCollection);
        assertTrue(store1.getConstraintCollections().isEmpty());

    }

    @Test
    public void testRemovalOfSchemaCascadesConstraintCollectionRemoval() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        Column col1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "foo", null, 1);
        Column col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        final ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null,
                col1, col2);

        NumberedDummyConstraint.buildAndAddToCollection(col1, dummyConstraintCollection, 100);

        store1.flush();

        store1.removeSchema(dummySchema1);
        assertTrue(store1.getConstraintCollections().isEmpty());
    }
}
