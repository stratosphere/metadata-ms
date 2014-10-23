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
import java.util.Arrays;
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
import de.hpi.isg.metadata_store.domain.factories.MetadataStoreFactory;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.DefaultConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

public class RDBMSMetadataStoreTest {

    private final File testDb = new File("/tmp/test.db");
    private Connection connection;

    @Before
    public void setUp() {
        try {
            this.testDb.createNewFile();
            // this.testDb.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:/tmp/test.db");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

        MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
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
                "INDpart", "Scope", "TYPEE" };
        Set<String> tables = new HashSet<String>(Arrays.asList(tableNames));

        try {
            meta = connection.getMetaData();
            ResultSet res = meta.getTables(null, null, null,
                    new String[] { "TABLE" });
            while (res.next()) {
                tables.remove(res.getString("TABLE_NAME"));
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
        final MetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        final Schema schema1 = store1.addSchema("pdb", mock(Location.class));
        assertTrue(store1.getSchemas().contains(schema1));
        assertTrue(store1.getAllTargets().contains(schema1));
    }

    @Test
    public void testConstructingAComplexSchema() {
        final MetadataStore metadataStore = new DefaultMetadataStore();
        for (int schemaNumber = 0; schemaNumber < 10; schemaNumber++) {
            final Schema schema = metadataStore.addSchema(String.format("schema-%03d", schemaNumber), null);
            for (int tableNumber = 0; tableNumber < 100; tableNumber++) {
                final Table table = schema.addTable(metadataStore, String.format("table-%03d", schemaNumber), null);
                for (int columnNumber = 0; columnNumber < 10; columnNumber++) {
                    table.addColumn(metadataStore, String.format("column-%03d", columnNumber), columnNumber);
                }
            }
        }
        final Collection<InclusionDependency> inclusionDependencies = new LinkedList<>();
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
                                final String name = String.format("IND[%s < %s]", dependentColumns, referencedColumns);
                                final InclusionDependency.Reference reference = new InclusionDependency.Reference(
                                        dependentColumns.toArray(new Column[dependentColumns.size()]),
                                        referencedColumns.toArray(new Column[referencedColumns.size()]));
                                final InclusionDependency inclusionDependency = new InclusionDependency(metadataStore,
                                        -1,
                                        name, reference);
                                inclusionDependencies.add(inclusionDependency);
                                if (inclusionDependencies.size() >= 300000) {
                                    break OuterLoop;
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println(String.format("Adding %d inclusion dependencies.", inclusionDependencies.size()));
        for (final InclusionDependency inclusionDependency : inclusionDependencies) {
            metadataStore.addConstraint(inclusionDependency);
        }
    }

    @Test
    public void testGetOrCreateOfExisting() {

        // setup store
        final RDBMSMetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema
        final Schema dummySchema = RDBMSSchema.buildAndRegisterAndAdd(store1, "PDB", new HDFSLocation("hdfs://foobar"));

        final HDFSLocation dummyTableLocation = new HDFSLocation("hdfs://foobar/dummyTable.csv");

        Column dummyColumn = dummySchema.addTable(store1, "dummyTable", dummyTableLocation).addColumn(store1,
                "dummyColumn", 0);

        final Constraint dummyContraint = new TypeConstraint(store1, new SingleTargetReference(
                dummyColumn));

        store1.addConstraint(dummyContraint);

        MetadataStore store2 = MetadataStoreFactory.getMetadataStoreFromSQLite(connection);

        assertEquals(store1, store2);
    }

    @Test
    public void testCreationOfEmptyRDBMSMetadataStore() {
        MetadataStore store2 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);

        assertEquals(store2, new RDBMSMetadataStore(new SQLiteInterface(connection)));
    }

    @Test
    public void testRetrievingOfSchemaByName() {
        // setup store
        final MetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", new HDFSLocation("hdfs://foobar"));

        assertEquals(store1.getSchema("PDB"), dummySchema1);
    }

    @Test
    public void testConstraintCollections() {
        // setup store
        final MetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", new HDFSLocation("hdfs://foobar"));
        store1.getSchemas().add(dummySchema1);
        Column col = dummySchema1.addTable(store1, "table1", mock(Location.class)).addColumn(store1, "foo", 1);
        final Set<?> scope = Collections.singleton(dummySchema1);
        final Constraint dummyTypeContraint = new TypeConstraint(store1, new SingleTargetReference(col));
        final Set<Constraint> constraints = Collections.singleton(dummyTypeContraint);

        @SuppressWarnings("unchecked")
        ConstraintCollection constraintCollection = new DefaultConstraintCollection(constraints, (Set<Target>) scope);

        store1.addConstraintCollection(constraintCollection);

        assertTrue(store1.getConstraintCollections().contains(constraintCollection));
        assertTrue(store1.getConstraints().contains(dummyTypeContraint));
    }

    @SuppressWarnings("unused")
    @Test(expected = NameAmbigousException.class)
    public void testRetrievingOfSchemaByNameWithAmbigousNameFails() {
        // setup store
        final MetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema
        final Schema dummySchema1 = store1.addSchema("PDB", new HDFSLocation("hdfs://foobar"));

        final Schema dummySchema2 = store1.addSchema("PDB", new HDFSLocation("hdfs://foobar"));

        store1.getSchema("PDB");
    }

    @Test
    public void testRetrievingOfSchemaByNameWithUnknownNameReturnsNull() {
        // setup store
        final MetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema

        assertEquals(store1.getSchema("PDB"), null);
    }

    @Test
    public void testStoringOfFilledMetadataStore() {
        // setup store
        final MetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", new HDFSLocation("hdfs://foobar"));

        final HDFSLocation dummyTableLocation = new HDFSLocation("hdfs://foobar/dummyTable.csv");

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", 0);

        final Constraint dummyContraint = new TypeConstraint(store1, new SingleTargetReference(
                dummyColumn));

        store1.addConstraint(dummyContraint);

        // retrieve store
        MetadataStore store2 = MetadataStoreFactory.getMetadataStoreFromSQLite(connection);

        assertEquals(dummySchema, store2.getSchemas().iterator().next());

        assertEquals(store1, store2);
    }

    @Test
    public void testStoringOfFilledMetadataStore2() {
        // setup store
        final RDBMSMetadataStore store1 = MetadataStoreFactory.createEmptyMetadataStoreInSQLite(connection);
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", new HDFSLocation("hdfs://foobar"));

        // retrieve store
        MetadataStore store2 = MetadataStoreFactory.getMetadataStoreFromSQLite(connection);

        assertEquals(store1, store2);

        final Schema schema = store2.getSchemas().iterator().next();

        assertEquals(dummySchema, schema);
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // end copied tests
    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
