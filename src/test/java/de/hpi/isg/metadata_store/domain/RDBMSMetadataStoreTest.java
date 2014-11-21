package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.factories.MetadataStoreFactory;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

public class RDBMSMetadataStoreTest {

    private File testDb;
    private Connection connection;

    @Before
    public void setUp() {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();
            // this.testDb.deleteOnExit();
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

        RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
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

    @Test
    public void testExistenceOfTables() {
        DatabaseMetaData meta;
        String[] tableNames = { "Target", "Schemaa", "Tablee", "Columnn", "ConstraintCollection", "Constraintt", "IND",
                "INDpart", "Scope", "Typee", "Location", "LocationProperty" };
        Set<String> tables = new HashSet<String>(Arrays.asList(tableNames));

        try {
            meta = connection.getMetaData();
            ResultSet res = meta.getTables(null, null, null,
                    new String[] { "TABLE" });
            while (res.next()) {
                // assertTrue(tables.remove(res.getString("TABLE_NAME")));
                if (!tables.remove(res.getString("TABLE_NAME"))) {
                    System.out.println(res.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertTrue(tables.isEmpty());
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // start copied tests
    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    @Test
    public void testAddingOfSchema() {
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        final Schema schema1 = store1.addSchema("pdb", new DefaultLocation());
        assertTrue(store1.getSchemas().contains(schema1));
        assertTrue(store1.getAllTargets().contains(schema1));
    }

    @Test
    public void testConstructingAComplexSchema() {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
        System.out.println("Creating schemas");
        final MetadataStore metadataStore = RDBMSMetadataStore.load(new SQLiteInterface(connection));
        for (int schemaNumber = 0; schemaNumber < 3; schemaNumber++) {
            final Schema schema = metadataStore.addSchema(String.format("schema-%03d", schemaNumber),
                    new DefaultLocation());
            for (int tableNumber = 0; tableNumber < 10; tableNumber++) {
                final Table table = schema.addTable(metadataStore, String.format("table-%03d", schemaNumber),
                        new DefaultLocation());
                for (int columnNumber = 0; columnNumber < 20; columnNumber++) {
                    Column column = table.addColumn(metadataStore, String.format("column-%03d", columnNumber),
                            columnNumber);
                }
            }
        }
        System.out.println("Created schemas");
        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
        System.out.println("Creating INDS");
        final Collection<InclusionDependency> inclusionDependencies = new LinkedList<>();
        ConstraintCollection constraintCollection = new RDBMSConstraintCollection(1, new HashSet<Constraint>(),
                new HashSet<Target>(), new SQLiteInterface(connection));
        int incNr = 0;
        final Random random = new Random();
        for (final Schema schema : metadataStore.getSchemas()) {
            OuterLoop: for (final Table table1 : schema.getTables()) {
                for (final Table table2 : schema.getTables()) {
                    for (final Column column1 : table1.getColumns()) {
                        for (final Column column2 : table2.getColumns()) {
                            List<Column> dependentColumns;
                            List<Column> referencedColumns;
                            if (column1 != column2 && random.nextInt(1000) <= 0) {
                                dependentColumns = Collections.singletonList(column1);
                                referencedColumns = Collections.singletonList(column2);
                                final InclusionDependency.Reference reference = new InclusionDependency.Reference(
                                        dependentColumns.toArray(new Column[dependentColumns.size()]),
                                        referencedColumns.toArray(new Column[referencedColumns.size()]));
                                final InclusionDependency inclusionDependency = InclusionDependency
                                        .buildAndAddToCollection(incNr++,
                                                reference, constraintCollection);
                                inclusionDependencies.add(inclusionDependency);
                                if (inclusionDependencies.size() >= 1000) {
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
        metadataStore.addConstraintCollection(constraintCollection);
        assertTrue(metadataStore.getConstraintCollections().iterator().next().getConstraints().size() == inclusionDependencies
                .size());
        System.out.println(dateFormat.format(Calendar.getInstance().getTime()));
    }

    @Test
    public void testGetOrCreateOfExisting() {

        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = RDBMSSchema.buildAndRegisterAndAdd(store1, "PDB", new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        Column dummyColumn = dummySchema.addTable(store1, "dummyTable", dummyTableLocation).addColumn(store1,
                "dummyColumn", 0);

        final Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(
                dummyColumn), mock(ConstraintCollection.class), TYPES.STRING);

        store1.addConstraint(dummyContraint);

        MetadataStore store2 = RDBMSMetadataStore.load(new SQLiteInterface(connection));

        assertEquals(store1, store2);
    }

    @Test
    public void testCreationOfEmptyRDBMSMetadataStore() {
        MetadataStore store2 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

        assertEquals(store2, RDBMSMetadataStore.load(new SQLiteInterface(connection)));
    }

    @Test
    public void testRetrievingOfSchemaByName() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", new DefaultLocation());

        assertEquals(store1.getSchema("PDB"), dummySchema1);
    }

    @Test
    public void testConstraintCollections() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", new DefaultLocation());
        Column col1 = dummySchema1.addTable(store1, "table1", new DefaultLocation()).addColumn(store1,
                "foo", 1);
        Column col2 = dummySchema1.addTable(store1, "table1", new DefaultLocation()).addColumn(store1,
                "bar", 2);

        final ConstraintCollection dummyConstraintCollection = new RDBMSConstraintCollection(1,
                new HashSet<Constraint>(),
                new HashSet<Target>(), new SQLiteInterface(
                        connection));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(1,
                new SingleTargetReference(col1),
                dummyConstraintCollection,
                TYPES.STRING);

        final Constraint dummyIndContraint = InclusionDependency.buildAndAddToCollection(2,
                new InclusionDependency.Reference(new Column[] { col1 }, new Column[] { col2 }),
                dummyConstraintCollection);

        store1.addConstraintCollection(dummyConstraintCollection);

        ConstraintCollection cc = store1.getConstraintCollections().iterator().next();

        cc.equals(dummyConstraintCollection);

        assertTrue(store1.getConstraintCollections().contains(dummyConstraintCollection));
        assertTrue(store1.getConstraintCollections().iterator().next().getConstraints().contains(dummyTypeContraint));
        assertTrue(store1.getConstraintCollections().iterator().next().getConstraints().contains(dummyIndContraint));
        assertTrue(store1.getConstraints().contains(dummyTypeContraint));
        assertTrue(store1.getConstraints().contains(dummyIndContraint));
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
        store1.addSchema("foo", mock(Location.class)).getTables().add(mock(Table.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetColumnsAddingFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        store1.addSchema("foo", new DefaultLocation()).addTable(store1, "bar", new DefaultLocation()).getColumns()
                .add(mock(Column.class));
    }

    @SuppressWarnings("unused")
    @Test(expected = NameAmbigousException.class)
    public void testRetrievingOfSchemaByNameWithAmbigousNameFails() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", new DefaultLocation());

        final Schema dummySchema2 = store1.addSchema("PDB", new DefaultLocation());

        store1.getSchema("PDB");
    }

    @Test
    public void testRetrievingOfSchemaByNameWithUnknownNameReturnsNull() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema

        assertEquals(store1.getSchema("PDB"), null);
    }

    @Test
    public void testStoringOfFilledMetadataStore() {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", 1);

        final Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(
                dummyColumn), mock(ConstraintCollection.class), TYPES.STRING);

        store1.addConstraint(dummyContraint);

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(new SQLiteInterface(connection));

        System.out.println(dummySchema.getLocation().equals(store2.getSchemas().iterator().next().getLocation()));

        assertEquals(dummySchema, store2.getSchemas().iterator().next());

        assertEquals(dummyColumn, store2.getSchemas().iterator().next().getTables().iterator().next().getColumns()
                .iterator().next());

        assertEquals(store1, store2);
    }

    @Test
    public void testStoringOfFilledMetadataStore2() {
        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", new DefaultLocation());

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(new SQLiteInterface(connection));

        assertEquals(store1, store2);

        final Schema schema = store2.getSchemas().iterator().next();

        assertEquals(dummySchema, schema);
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // end copied tests
    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
