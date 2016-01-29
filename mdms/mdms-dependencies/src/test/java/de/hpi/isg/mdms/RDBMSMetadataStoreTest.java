package de.hpi.isg.mdms;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.domain.util.SQLiteConstraintUtils;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RDBMSMetadataStoreTest {

    private static final int loadFactorForCreateComplexSchemaTest = 7;

    private File testDb;
    private Connection connection;

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());

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

    @Test
    public void testStoringTupleCountConstraint() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        final TupleCount dummyContraint = TupleCount.buildAndAddToCollection(new SingleTargetReference(
                dummyTable.getId()), constraintCollection, 5);
        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteConstraintUtils.registerStandardConstraints(new SQLiteInterface(connection)));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringTypeConstraint() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", null, 1);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        final TypeConstraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()), constraintCollection, "VARCHAR");
        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteConstraintUtils.registerStandardConstraints(new SQLiteInterface(connection)));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringDistinctCountConstraint() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn = dummyTable.addColumn(store1, "dummyColumn", null, 1);

        ConstraintCollection constraintCollection = store1.createConstraintCollection("some collection");

        final DistinctValueCount dummyContraint = DistinctValueCount.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()), constraintCollection, 5);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteConstraintUtils.registerStandardConstraints(new SQLiteInterface(connection)));

        ConstraintCollection loadedCollection1 = store1.getConstraintCollections().iterator().next();
        ConstraintCollection loadedCollection2 = store2.getConstraintCollections().iterator().next();
        assertTrue("Original constraint collection is empty.", !loadedCollection1.getConstraints().isEmpty());
        assertTrue("Loaded constraint collection is empty.", !loadedCollection2.getConstraints().isEmpty());
        assertEquals(loadedCollection1.getConstraints().iterator().next(),
                loadedCollection2.getConstraints().iterator().next());
    }

    @Test
    public void testStoringInclusionDependency() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn1 = dummyTable.addColumn(store1, "dummyColumn1", null, 1);
        final Column dummyColumn2 = dummyTable.addColumn(store1, "dummyColumn2", null, 2);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);

        final InclusionDependency dummyContraint = InclusionDependency.buildAndAddToCollection(
                new InclusionDependency.Reference(
                        new Column[]{dummyColumn1}, new Column[]{dummyColumn2}), constraintCollection);

        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteConstraintUtils.registerStandardConstraints(new SQLiteInterface(connection)));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testUniqueColumnCombination() throws Exception {
        // setup store
        final MetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = dummySchema.addTable(store1, "dummyTable", null, dummyTableLocation);

        final Column dummyColumn1 = dummyTable.addColumn(store1, "dummyColumn1", null, 1);
        final Column dummyColumn2 = dummyTable.addColumn(store1, "dummyColumn2", null, 2);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);

        final UniqueColumnCombination dummyContraint = UniqueColumnCombination.buildAndAddToCollection(
                new UniqueColumnCombination.Reference(
                        new int[]{dummyColumn1.getId(), dummyColumn2.getId()}), constraintCollection);

        constraintCollection.add(dummyContraint);

        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteConstraintUtils.registerStandardConstraints(new SQLiteInterface(connection)));

        assertEquals(store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(),
                store2.getConstraintCollections().iterator().next().getConstraints().iterator().next());
    }

    @Test
    public void testStoringOfFilledMetadataStore2() throws Exception {
        // setup store
        final RDBMSMetadataStore store1 = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));
        // setup schema
        final Schema dummySchema = store1.addSchema("PDB", null, new DefaultLocation());
        store1.flush();

        // retrieve store
        MetadataStore store2 = RDBMSMetadataStore.load(SQLiteConstraintUtils.registerStandardConstraints(new SQLiteInterface(connection)));

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

        TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(col1.getId()),
                dummyConstraintCollection,
                "VARCHAR");

        DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(col1.getId()),
                dummyConstraintCollection,
                1);

        InclusionDependency.buildAndAddToCollection(new
                InclusionDependency.Reference(new Column[]{col1}, new Column[]{col2}), dummyConstraintCollection);

        TupleCount.buildAndAddToCollection(new SingleTargetReference(
                        dummyTable1.getId()),
                dummyConstraintCollection, 1);

        UniqueColumnCombination.buildAndAddToCollection(new
                UniqueColumnCombination.Reference(new int[]{col1.getId()}), dummyConstraintCollection);

        DistinctValueOverlap
                .buildAndAddToCollection(1, new
                                DistinctValueOverlap.Reference(1, 2),
                        dummyConstraintCollection);

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

        TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(col1.getId()),
                dummyConstraintCollection,
                "VARCHAR");

        DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(col1.getId()),
                dummyConstraintCollection,
                1);

        InclusionDependency.buildAndAddToCollection(new
                InclusionDependency.Reference(new Column[]{col1}, new Column[]{col2}), dummyConstraintCollection);

        TupleCount.buildAndAddToCollection(new SingleTargetReference(
                        dummyTable1.getId()),
                dummyConstraintCollection, 1);

        DistinctValueOverlap
                .buildAndAddToCollection(1, new
                                DistinctValueOverlap.Reference(1, 2),
                        dummyConstraintCollection);

        /*
         * dummyUCCConstraint = UniqueColumnCombination.buildAndAddToCollection( new
         * UniqueColumnCombination.Reference(new Column[] { col1 }), dummyConstraintCollection);
         */

        store1.removeConstraintCollection(dummyConstraintCollection);
        assertTrue(store1.getConstraintCollections().isEmpty());

    }

}
