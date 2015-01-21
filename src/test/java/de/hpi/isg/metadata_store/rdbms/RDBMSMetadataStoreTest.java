package de.hpi.isg.metadata_store.rdbms;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.constraints.impl.DistinctValueCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.TupleCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.constraints.impl.UniqueColumnCombination;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

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

        // RDBMSMetadataStore.createNewInstance(SQLiteInterface.buildAndRegisterStandardConstraints(connection));
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
                    new String[] { "TABLE" });
            while (res.next()) {
                // assertTrue(tables.remove(res.getString("TABLE_NAME")));
                if (!tables.remove(res.getString("TABLE_NAME").toLowerCase())) {
                    System.out.println("Unexpected table: " + res.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertTrue(tables.isEmpty());
    }

    @Test
    public void testAddingOfSchema() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", "foobar", new DefaultLocation());
        assertTrue(store1.getSchemas().contains(schema1));
        assertTrue(store1.getAllTargets().contains(schema1));
    }

    @Test
    public void testConstructingAComplexSchema() {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
        System.out.println("Creating schemas");
        final MetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        for (int schemaNumber = 0; schemaNumber < 1 * loadFactorForCreateComplexSchemaTest; schemaNumber++) {
            final Schema schema = metadataStore.addSchema(String.format("schema-%03d", schemaNumber), null,
                    new DefaultLocation());
            for (int tableNumber = 0; tableNumber < 2 * loadFactorForCreateComplexSchemaTest; tableNumber++) {
                final Table table = schema.addTable(metadataStore, String.format("table-%03d", schemaNumber), null,
                        new DefaultLocation());
                for (int columnNumber = 0; columnNumber < 3 * loadFactorForCreateComplexSchemaTest; columnNumber++) {
                    Column column = table.addColumn(metadataStore, String.format("column-%03d", columnNumber), null,
                            columnNumber);
                }
            }
        }
        System.out.println("Created schemas");
        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
        System.out.println("Creating INDS");
        final Collection<InclusionDependency> inclusionDependencies = new LinkedList<>();
        ConstraintCollection constraintCollection = metadataStore.createConstraintCollection("", metadataStore
                .getSchemas().toArray(new Schema[metadataStore.getSchemas().size()]));
        int incNr = 0;
        final Random random = new Random();
        for (final Schema schema : metadataStore.getSchemas()) {
            OuterLoop: for (final Table table1 : schema.getTables()) {
                for (final Table table2 : schema.getTables()) {
                    for (final Column column1 : table1.getColumns()) {
                        for (final Column column2 : table2.getColumns()) {
                            List<Column> dependentColumns;
                            List<Column> referencedColumns;
                            if (column1 != column2 && random.nextInt(10 * loadFactorForCreateComplexSchemaTest) <= 0) {
                                dependentColumns = Collections.singletonList(column1);
                                referencedColumns = Collections.singletonList(column2);
                                final InclusionDependency.Reference reference = new InclusionDependency.Reference(
                                        dependentColumns.toArray(new Column[dependentColumns.size()]),
                                        referencedColumns.toArray(new Column[referencedColumns.size()]));
                                final InclusionDependency inclusionDependency = InclusionDependency
                                        .buildAndAddToCollection(
                                                reference, constraintCollection);
                                inclusionDependencies.add(inclusionDependency);
                                if (inclusionDependencies.size() >= 10 * loadFactorForCreateComplexSchemaTest) {
                                    break OuterLoop;
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
        System.out.println(String.format("Adding %d inclusion dependencies.", inclusionDependencies.size()));
        assertTrue(metadataStore.getConstraintCollections().iterator().next().getConstraints().size() == inclusionDependencies
                .size());
        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
    }

    @Test
    public void testGetOrCreateOfExisting() throws Exception {

        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        // final Schema dummySchema = RDBMSSchema.buildAndRegisterAndAdd(store1, "PDB", new DefaultLocation());
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        Column dummyColumn = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation).addColumn(store1,
                "dummyColumn", null, 0);

        final Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn), mock(ConstraintCollection.class), TYPES.STRING);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        constraintCollection.add(dummyContraint);

        store1.flush();

        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1, store2);
    }

    @Test
    public void testCreationOfEmptyRDBMSMetadataStore() throws Exception {
        MetadataStore store2 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        store2.flush();

        assertEquals(store2, RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection)));
    }

    @Test
    public void testRetrievingOfSchemaByName() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());

        assertEquals(store1.getSchemaByName("PDB"), dummySchema1);
    }

    @Test
    public void testConstraintCollections() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        Column col1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "foo", null, 1);
        Column col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        final ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema1);

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                TYPES.STRING);

        final Constraint dummyIndContraint = InclusionDependency.buildAndAddToCollection(
                new InclusionDependency.Reference(new Column[] { col1 }, new Column[] { col2 }),
                dummyConstraintCollection);

        ConstraintCollection cc = store1.getConstraintCollections().iterator().next();
        // store1.flush();

        assertTrue(store1.getConstraintCollections().contains(dummyConstraintCollection));
        assertTrue(store1.getConstraintCollections().iterator().next().getConstraints().contains(dummyTypeContraint));
        assertTrue(store1.getConstraintCollections().iterator().next().getConstraints().contains(dummyIndContraint));
    }

    @Test
    public void testUnregisteredLocationTypeIsDeserializedAsdefaultLocation() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        store1.addSchema("Foobar", "description", new Location() {

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
        });

        assertEquals(DefaultLocation.class, store1.getSchemaByName("Foobar").getLocation().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSchemasAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        store1.getSchemas().add(mock(Schema.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTablesAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        store1.addSchema("foo", null, mock(Location.class)).getTables().add(mock(Table.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetColumnsAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        store1.addSchema("foo", null, new DefaultLocation()).addTable(store1, "bar", null, new DefaultLocation())
                .getColumns()
                .add(mock(Column.class));
    }

    @Test
    public void testRetrievingOfSchemaByNameWithUnknownNameReturnsNull() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema

        assertEquals(store1.getSchemaByName("PDB"), null);
    }

    @Test
    public void testStoringOfFilledMetadataStore() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", null, 1);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null, dummySchema);
        final Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn), mock(ConstraintCollection.class), TYPES.STRING);
        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(dummySchema, store2.getSchemas().iterator().next());

        assertEquals(dummyColumn, store2.getSchemas().iterator().next().getTables().iterator().next().getColumns()
                .iterator().next());

        assertEquals(store1, store2);
    }

    @Test
    public void testStoringTupleCountConstraint() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        final TupleCount dummyContraint = TupleCount.buildAndAddToCollection(new SingleTargetReference(
                dummyTable), constraintCollection, 5);
        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringTypeConstraint() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", null, 1);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        final TypeConstraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn), constraintCollection, TypeConstraint.TYPES.STRING);
        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringDistinctCountConstraint() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", null, 1);

        ConstraintCollection constraintCollection = store1.createConstraintCollection("some collection");

        final DistinctValueCount dummyContraint = DistinctValueCount.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn), constraintCollection, 5);

        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringInclusionDependency() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn1 = dummyTable.addColumn(store1, "dummyColumn1", null, 1);
        final Column dummyColumn2 = dummyTable.addColumn(store1, "dummyColumn2", null, 2);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);

        final InclusionDependency dummyContraint = InclusionDependency.buildAndAddToCollection(
                new InclusionDependency.Reference(
                        new Column[] { dummyColumn1 }, new Column[] { dummyColumn2 }), constraintCollection);

        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testUniqueColumnCombination() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn1 = dummyTable.addColumn(store1, "dummyColumn1", null, 1);
        final Column dummyColumn2 = dummyTable.addColumn(store1, "dummyColumn2", null, 2);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);

        final UniqueColumnCombination dummyContraint = UniqueColumnCombination.buildAndAddToCollection(
                new UniqueColumnCombination.Reference(
                        new Column[] { dummyColumn1, dummyColumn2 }), constraintCollection);

        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringOfFilledMetadataStore2() throws Exception {
        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());
        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteInterface.buildAndRegisterStandardConstraints(connection));

        assertEquals(store1, store2);

        final Schema schema = store2.getSchemas().iterator().next();

        assertEquals(dummySchema, schema);
    }

    @Test
    public void testGettingOfSchemaByNameAndId() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());

        assertEquals(schema1, store1.getSchemaByName("pdb"));
        assertEquals(schema1, store1.getSchemaById(schema1.getId()));
    }

    @Test
    public void testGettingOfSchemasByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Schema schema2 = store1.addSchema("pdb", null, new DefaultLocation());
        HashSet<Schema> schemas = new HashSet<>();
        schemas.add(schema1);
        schemas.add(schema2);
        assertEquals(schemas, store1.getSchemasByName("pdb"));
    }

    @Test(expected = NameAmbigousException.class)
    public void testGettingOfSchemaByNameFails() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Schema schema2 = store1.addSchema("pdb", null, new DefaultLocation());
        store1.getSchemaByName("pdb");
    }

    @Test
    public void testGettingOfTableByNameAndId() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());

        assertEquals(table1, schema1.getTableByName("foo"));
        assertEquals(table1, schema1.getTableById(table1.getId()));
    }

    @Test
    public void testGettingOfTablesByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
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
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Table table2 = schema1.addTable(store1, "foo", null, new DefaultLocation());

        schema1.getTableByName("foo");
    }

    @Test
    public void testGettingOfColumnByNameAndId() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", "comment", new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", "ccc", 0);

        assertEquals(column1, table1.getColumnByName("bar"));
        assertEquals(column1, table1.getColumnById(column1.getId()));
    }

    @Test
    public void testGettingOfColumnssByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", null, 0);
        final Collection<Column> columns = new HashSet<>();
        columns.add(column1);

        assertEquals(columns, table1.getColumnsByName("bar"));
    }

    @Test(expected = NameAmbigousException.class)
    public void testGettingOfColumnByNameFails() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", null, 0);
        final Column column2 = table1.addColumn(store1, "bar", null, 1);

        table1.getColumnByName("bar");
    }

    @Test
    public void testGettingOfColumnByName() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        final Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        final Table table1 = schema1.addTable(store1, "foo1", null, new DefaultLocation());
        final Table table2 = schema1.addTable(store1, "foo2", null, new DefaultLocation());
        final Column column1 = table1.addColumn(store1, "bar", null, 0);
        final Column column2 = table2.addColumn(store1, "bar", null, 1);

        assertEquals(column1, table1.getColumnByName("bar"));
    }

    @Test
    public void testRemovalOfSchema() throws Exception {
        MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        Schema schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        Table table1 = schema1.addTable(store1, "foo1", null, new DefaultLocation());
        Table table2 = schema1.addTable(store1, "foo2", null, new DefaultLocation());

        Column column1 = table1.addColumn(store1, "bar1", null, 0);
        Column column2 = table1.addColumn(store1, "bar2", null, 1);
        Column column3 = table2.addColumn(store1, "bar3", null, 0);

        assertTrue(!store1.getSchemas().isEmpty());

        store1.removeSchema(schema1);

        assertTrue(store1.getSchemas().isEmpty());

        schema1 = store1.addSchema("pdb", null, new DefaultLocation());
        table1 = schema1.addTable(store1, "foo1", null, new DefaultLocation());
        table2 = schema1.addTable(store1, "foo2", null, new DefaultLocation());

        column1 = table1.addColumn(store1, "bar", null, 0);
        column2 = table1.addColumn(store1, "bar", null, 1);
        column3 = table2.addColumn(store1, "bar", null, 0);

        store1.flush();

        store1.removeSchema(schema1);

        assertTrue(store1.getSchemas().isEmpty());
    }

    @Test
    public void testRemovalOfConstraintCollections() throws Exception {
        // setup store
        MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        Table dummyTable1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation());

        Column col1 = dummyTable1.addColumn(store1, "foo", null, 1);
        Column col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema1);

        Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                TYPES.STRING);

        Constraint dummyDistinctValueCount = DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                1);

        Constraint dummyIndContraint = InclusionDependency.buildAndAddToCollection(new
                InclusionDependency.Reference(new Column[] { col1 }, new Column[] { col2 }), dummyConstraintCollection);

        Constraint dummyTupleCountContraint = TupleCount.buildAndAddToCollection(new SingleTargetReference(
                dummyTable1),
                dummyConstraintCollection, 1);

        Constraint dummyUCCConstraint = UniqueColumnCombination.buildAndAddToCollection(new
                UniqueColumnCombination.Reference(new Column[] { col1 }), dummyConstraintCollection);

        store1.removeConstraintCollection(dummyConstraintCollection);
        assertTrue(store1.getConstraintCollections().isEmpty());

        // setup store
        store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        dummyTable1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation());

        col1 = dummyTable1.addColumn(store1, "foo", null, 1);
        col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema1);

        dummyTypeContraint = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                TYPES.STRING);

        dummyDistinctValueCount = DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                1);

        dummyIndContraint = InclusionDependency.buildAndAddToCollection(new
                InclusionDependency.Reference(new Column[] { col1 }, new Column[] { col2 }), dummyConstraintCollection);

        dummyTupleCountContraint = TupleCount.buildAndAddToCollection(new SingleTargetReference(
                dummyTable1),
                dummyConstraintCollection, 1);

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
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", null, new DefaultLocation());
        Column col1 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "foo", null, 1);
        Column col2 = dummySchema1.addTable(store1, "table1", null, new DefaultLocation()).addColumn(store1,
                "bar", null, 2);

        final ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null,
                col1, col2);

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                TYPES.STRING);

        final Constraint dummyIndContraint = InclusionDependency.buildAndAddToCollection(
                new InclusionDependency.Reference(new Column[] { col1 }, new Column[] { col2 }),
                dummyConstraintCollection);

        store1.flush();

        store1.removeSchema(dummySchema1);
        assertTrue(store1.getConstraintCollections().isEmpty());
    }
}
