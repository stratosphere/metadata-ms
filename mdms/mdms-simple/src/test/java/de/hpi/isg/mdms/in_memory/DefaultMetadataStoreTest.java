package de.hpi.isg.mdms.in_memory;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.exceptions.IdAlreadyInUseException;
import de.hpi.isg.mdms.exceptions.MetadataStoreNotFoundException;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.DefaultSchema;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.simple.factories.DefaultMetadataStoreFactory;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DefaultMetadataStoreTest {

    private static final int loadFactorForCreateComplexSchemaTest = 1;

    private final File dir = new File("test/");

    @Before
    public void setUp() {
        this.dir.mkdir();
        try {
            FileUtils.cleanDirectory(this.dir);
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            FileUtils.cleanDirectory(this.dir);
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddingOfSchema() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Schema schema1 = store1.addSchema("pdb", "foo", mock(Location.class));
        assertTrue(store1.getSchemas().contains(schema1));
    }

    @Test
    public void testGenerationOfIds() {
        final MetadataStore store = new DefaultMetadataStore();
        store.addSchema("foo", null, mock(Location.class)).addTable(store, "bar", null, mock(Location.class))
                .addColumn(store, "column1", null, 0);
        assertEquals(0b111111111111111111111111, store.getSchemaByName("foo").getId());
        assertEquals(0b000000000000111111111111, store.getSchemaByName("foo").getTableByName("bar").getId());
        assertEquals(0b000000000000000000000000, store.getSchemaByName("foo").getTableByName("bar").getColumns()
                .iterator().next()
                .getId());

        store.addSchema("foo2", null, mock(Location.class)).addTable(store, "bar2", null, mock(Location.class))
                .addColumn(store, "column1", null, 0);
        assertEquals(0b1111111111111111111111111, store.getSchemaByName("foo2").getId());
        assertEquals(0b1000000000000111111111111, store.getSchemaByName("foo2").getTableByName("bar2").getId());
        assertEquals(0b1000000000000000000000000, store.getSchemaByName("foo2").getTableByName("bar2").getColumns()
                .iterator()
                .next().getId());
    }

    @Test
    @Ignore
    public void testConstructingAComplexSchema() {
        final MetadataStore metadataStore = new DefaultMetadataStore();
        for (int schemaNumber = 0; schemaNumber <= Math.min(3, metadataStore.getIdUtils().getMaxSchemaNumber()); schemaNumber++) {
            final Schema schema = metadataStore.addSchema(String.format("schema-%03d", schemaNumber), null, null);
            for (int tableNumber = 0; tableNumber < 100 * loadFactorForCreateComplexSchemaTest; tableNumber++) {
                final Table table = schema.addTable(metadataStore, String.format("table-%03d", schemaNumber),
                        null, null);
                for (int columnNumber = 0; columnNumber < 10 * loadFactorForCreateComplexSchemaTest; columnNumber++) {
                    table.addColumn(metadataStore, String.format("column-%03d", columnNumber), null, columnNumber);
                }
            }
        }

        System.out.println("Adding inclusion dependencies.");
        final Random random = new Random();
        for (final Schema schema : metadataStore.getSchemas()) {
            ConstraintCollection<InclusionDependency> constraintCollection =
                    metadataStore.createConstraintCollection(null, InclusionDependency.class, schema);
            int numInclusionDependencies = 0;
            OuterLoop:
            for (final Table table1 : schema.getTables()) {
                for (final Table table2 : schema.getTables()) {
                    for (final Column column1 : table1.getColumns()) {
                        for (final Column column2 : table2.getColumns()) {
                            List<Column> dependentColumns;
                            List<Column> referencedColumns;
                            if (column1 != column2 && random.nextInt(10 * loadFactorForCreateComplexSchemaTest) <= 0) {
                                InclusionDependency testConstraint = new InclusionDependency(column1.getId(), column2.getId());
                                constraintCollection.add(testConstraint);
                                numInclusionDependencies++;
                                if (numInclusionDependencies >= 3000 * loadFactorForCreateComplexSchemaTest) {
                                    break OuterLoop;
                                }
                            }
                        }
                    }
                }
                System.out.println(String.format("Added %d inclusion dependencies.", numInclusionDependencies));
            }
        }
    }

    @Test(expected = MetadataStoreNotFoundException.class)
    public void testGetMetaDataStoreOnNotExistingFails() {
        final File file = new File(this.dir, "nooooootExisting.ms");

        DefaultMetadataStoreFactory.loadDefaultMetadataStore(file);
    }

    /*
     * @Test public void testGetOrCreateOfExisting() { final File file = new File(this.dir, "filledStore.ms"); // setup
     * store final DefaultMetadataStore store1 = new DefaultMetadataStore(); // setup schema final Schema dummySchema =
     * DefaultSchema.buildAndRegister(store1, "PDB", null, new DefaultLocation()); final DefaultLocation
     * dummyTableLocation = new DefaultLocation(); final Table dummyTable = DefaultTable.buildAndRegister(store1,
     * dummySchema, "dummyTable", null, dummyTableLocation); final Column dummyColumn =
     * DefaultColumn.buildAndRegister(store1, dummyTable, "dummyColumn", null, new DefaultLocation()); final
     * ConstraintCollection cC = store1.createConstraintCollection(null, dummySchema); final Constraint dummyContraint =
     * TypeConstraint.buildAndAddToCollection(new SingleTargetReference( dummyColumn.getId()), cC, TYPES.STRING);
     * store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn))); try {
     * store1.save(file.getAbsolutePath()); } catch (final IOException e) { // TODO Auto-generated catch block
     * e.printStackTrace(); } MetadataStore store2 = null; try { store2 =
     * MetadataStoreFactory.loadOrCreateAndSaveDefaultMetadataStore(file); } catch (MetadataStoreNotFoundException |
     * IOException e) { // TODO Auto-generated catch block e.printStackTrace(); } assertEquals(store1, store2); }
     */

    @Test(expected = MetadataStoreNotFoundException.class)
    public void testGetOrCreateOfNotExisting() {
        final File file = new File(this.dir, "notExisting.ms");

        MetadataStore store2 = null;
        store2 = DefaultMetadataStoreFactory.loadDefaultMetadataStore(file);

        assertEquals(store2, new DefaultMetadataStore());
    }

    @Test
    public void testRetrievingOfSchemaByName() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", null, mock(Location.class));
        store1.getSchemas().add(dummySchema1);

        assertEquals(store1.getSchemaByName("PDB"), dummySchema1);
    }

    @Test
    public void testConstaintCollections() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", null, mock(Location.class));
        store1.getSchemas().add(dummySchema1);
        Column col = dummySchema1.addTable(store1, "table1", null, mock(Location.class)).addColumn(store1, "foo", null,
                1);

        @SuppressWarnings("unused")
        final Set<?> scope = Collections.singleton(dummySchema1);
        final InclusionDependency testConstraint = new InclusionDependency(col.getId(), col.getId());

        ConstraintCollection<InclusionDependency> constraintCollection =
                store1.createConstraintCollection(null, InclusionDependency.class, dummySchema1);
        constraintCollection.add(testConstraint);

        assertTrue(store1.getConstraintCollections().contains(constraintCollection));
    }

    @Test(expected = NameAmbigousException.class)
    public void testRetrievingOfSchemaByNameWithAmbigousNameFails() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", null, mock(Location.class));
        store1.getSchemas().add(dummySchema1);

        final Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, "PDB", null, mock(Location.class));
        store1.getSchemas().add(dummySchema2);

        store1.getSchemaByName("PDB");
    }

    @Test
    public void testRetrievingOfSchemaByNameWithUnknownNameReturnsNull() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema

        assertEquals(store1.getSchemaByName("PDB"), null);
    }

    @Test
    public void testStoringOfEmptyMetadataStore() {
        final File file = new File(this.dir, "emptyStore.ms");
        final DefaultMetadataStore store1 = new DefaultMetadataStore();
        try {
            store1.save(file.getAbsolutePath());
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        MetadataStore store2 = null;
        try {
            store2 = DefaultMetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertEquals(store1, store2);
    }

    @Test
    public void testGetTargetByName() {
        final MetadataStore metadataStore = new DefaultMetadataStore();
        Schema a = metadataStore.addSchema("a", "", new DefaultLocation());
        Table aa = a.addTable(metadataStore, "a", "", new DefaultLocation());
        Column aaa = aa.addColumn(metadataStore, "a", "", 0);
        Column aab = aa.addColumn(metadataStore, "b", "", 1);
        Table ab = a.addTable(metadataStore, "b", "", new DefaultLocation());
        Column aba = ab.addColumn(metadataStore, "a", "", 0);
        Column abb = ab.addColumn(metadataStore, "b", "", 1);
        Schema b = metadataStore.addSchema("b", "", new DefaultLocation());
        Table ba = b.addTable(metadataStore, "a", "", new DefaultLocation());
        Column baa = ba.addColumn(metadataStore, "a", "", 0);
        Column bab = ba.addColumn(metadataStore, "b", "", 1);
        Table bb = b.addTable(metadataStore, "b", "", new DefaultLocation());
        Column bba = bb.addColumn(metadataStore, "a", "", 0);
        Column bbb = bb.addColumn(metadataStore, "b", "", 1);

        Assert.assertEquals(a, metadataStore.getTargetByName("a"));
        Assert.assertEquals(aa, metadataStore.getTargetByName("a.a"));
        Assert.assertEquals(aaa, metadataStore.getTargetByName("a.a.a"));
        Assert.assertEquals(aab, metadataStore.getTargetByName("a.a.b"));
        Assert.assertEquals(ab, metadataStore.getTargetByName("a.b"));
        Assert.assertEquals(aba, metadataStore.getTargetByName("a.b.a"));
        Assert.assertEquals(abb, metadataStore.getTargetByName("a.b.b"));
        Assert.assertEquals(b, metadataStore.getTargetByName("b"));
        Assert.assertEquals(ba, metadataStore.getTargetByName("b.a"));
        Assert.assertEquals(baa, metadataStore.getTargetByName("b.a.a"));
        Assert.assertEquals(bab, metadataStore.getTargetByName("b.a.b"));
        Assert.assertEquals(bb, metadataStore.getTargetByName("b.b"));
        Assert.assertEquals(bba, metadataStore.getTargetByName("b.b.a"));
        Assert.assertEquals(bbb, metadataStore.getTargetByName("b.b.b"));

        Assert.assertEquals(null, metadataStore.getTargetByName("c"));
        Assert.assertEquals(null, metadataStore.getTargetByName("c"));
        Assert.assertEquals(null, metadataStore.getTargetByName("a.c"));
        Assert.assertEquals(null, metadataStore.getTargetByName("c.c"));
        Assert.assertEquals(null, metadataStore.getTargetByName("a.c.a"));
        Assert.assertEquals(null, metadataStore.getTargetByName("c.c.a"));
        Assert.assertEquals(null, metadataStore.getTargetByName("a.c.c"));
        Assert.assertEquals(null, metadataStore.getTargetByName("c.c.c"));
    }

    @Test(expected = NameAmbigousException.class)
    public void testGetTargetByNameWithAmbigousName() {
        final MetadataStore metadataStore = new DefaultMetadataStore();
        Schema a = metadataStore.addSchema("a", "", new DefaultLocation());
        a.addTable(metadataStore, "a", "", new DefaultLocation());
        metadataStore.addSchema("a.a", "", new DefaultLocation());
        metadataStore.getTargetByName("a.a");
    }

    /*
     * @Test public void testStoringOfFilledMetadataStore() { final File file = new File(this.dir, "filledStore.ms"); //
     * setup store final DefaultMetadataStore store1 = new DefaultMetadataStore(); // setup schema final Schema
     * dummySchema = DefaultSchema.buildAndRegister(store1, "PDB", null, new DefaultLocation()); final DefaultLocation
     * dummyTableLocation = new DefaultLocation(); final Table dummyTable = DefaultTable.buildAndRegister(store1,
     * dummySchema, "dummyTable", null, dummyTableLocation); final Column dummyColumn =
     * DefaultColumn.buildAndRegister(store1, dummyTable, "dummyColumn", null, new DefaultLocation()); final
     * ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(null, dummySchema); final
     * Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
     * dummyColumn.getId()), dummyConstraintCollection, TYPES.STRING); // XXX Do we need this line?
     * store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn))); try {
     * store1.save(file.getAbsolutePath()); } catch (final IOException e) { // TODO Auto-generated catch block
     * e.printStackTrace(); } // retrieve store MetadataStore store2 = null; try { store2 =
     * MetadataStoreFactory.load(file); } catch (final MetadataStoreNotFoundException e) { // TODO
     * Auto-generated catch block e.printStackTrace(); } assertEquals(dummySchema,
     * store2.getSchemas().iterator().next()); Constraint cc1 =
     * store1.getConstraintCollections().iterator().next().getConstraints().iterator().next(); Constraint cc2 =
     * store2.getConstraintCollections().iterator().next().getConstraints().iterator().next(); cc1.equals(cc2);
     * assertEquals(cc1, cc2); assertEquals(store1, store2); }
     */

    @Test
    public void testStoringOfFilledMetadataStore2() {
        final File file = new File(this.dir, "filledStore.ms");
        // setup store
        final DefaultMetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "PDB", null, new DefaultLocation());
        store1.getSchemas().add(dummySchema);

        try {
            store1.save(file.getAbsolutePath());
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // retrieve store
        MetadataStore store2 = null;

        try {
            store2 = DefaultMetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertEquals(store1, store2);

        final Schema schema = store2.getSchemas().iterator().next();

        assertEquals(dummySchema, schema);
    }

    /*
     * @Test public void testStoringOfFilledMetadataStore3() { final File file = new File(this.dir, "filledStore.ms");
     * // setup store final DefaultMetadataStore store1 = new DefaultMetadataStore(); // setup schema final Schema
     * dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", null, new DefaultLocation())
     * .addTable(DefaultTable.buildAndRegister(store1, mock(Schema.class), "foo", null, null));
     * store1.getSchemas().add(dummySchema1); try { store1.save(file.getAbsolutePath()); } catch (final IOException e) {
     * // TODO Auto-generated catch block e.printStackTrace(); } // retrieve store MetadataStore store2 = null; try {
     * store2 = MetadataStoreFactory.load(file); } catch (final MetadataStoreNotFoundException e) {
     * // TODO Auto-generated catch block e.printStackTrace(); } assertEquals(store1, store2); }
     */

    @Test
    public void testAddingOfAlgorithm() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Algorithm algorithm1 = store1.createAlgorithm("algorithm1");
        assertTrue(store1.getAlgorithms().contains(algorithm1));
    }

    @Test
    public void testRetrievingOfAlgorithm() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Algorithm algorithm1 = store1.createAlgorithm("algorithm1");
        assertTrue(store1.getAlgorithmById(algorithm1.getId()) == algorithm1);
    }

    @Test
    public void testCreatingOfExperiment() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Algorithm algorithm1 = store1.createAlgorithm("algorithm1");
        Experiment experiment = store1.createExperiment("experiment1", algorithm1);
        assertTrue(store1.getExperiments().contains(experiment));
    }

    @Test
    public void testRetrievingOfExperiment() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Algorithm algorithm1 = store1.createAlgorithm("algorithm1");
        Experiment experiment = store1.createExperiment("experiment1", algorithm1);
        experiment.addParameter("key", "value");
        assertTrue(store1.getExperimentById(experiment.getId()) == experiment);
        assertTrue(store1.getExperimentById(experiment.getId()).getParameters().size() == 1);
    }

    @Test
    public void testAddConstraintCollectionToExperiment() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", null, mock(Location.class));
        store1.getSchemas().add(dummySchema1);
        final Algorithm algorithm1 = store1.createAlgorithm("algorithm1");
        Experiment experiment = store1.createExperiment("experiment1", algorithm1);
        ConstraintCollection<InclusionDependency> constraintCollection =
                store1.createConstraintCollection(null, InclusionDependency.class, dummySchema1);
        experiment.add(constraintCollection);
        assertTrue(store1.getExperimentById(experiment.getId()).getConstraintCollections().size() == 1);
    }

    @Test
    public void testGetConstraintCollectionByTarget() {
        // XY in S2?
        {
            final MetadataStore store1 = new DefaultMetadataStore();
            final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "schema1", null, mock(Location.class));

            store1.getSchemas().add(dummySchema);
            ConstraintCollection<InclusionDependency> constraintCollection =
                    store1.createConstraintCollection(null, InclusionDependency.class, dummySchema);

            Table dummyTable = dummySchema.addTable(store1, "table1", null, mock(Location.class));
            Column dummyCol = dummyTable.addColumn(store1, "col1", null, 1);

            final Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, "schema2", null, mock(Location.class));
            store1.getSchemas().add(dummySchema2);
            Table dummyTable2 = dummySchema2.addTable(store1, "table2", null, mock(Location.class));
            Column dummyCol2 = dummyTable2.addColumn(store1, "col2", null, 1);

            Collection<ConstraintCollection<?>> result;

            // S1 in S2?
            result = store1.getConstraintCollectionByTarget(dummySchema);
            assertEquals(1, result.size());
            for (Object o : result) {
                assertTrue(store1.getConstraintCollections().contains(o));
            }
            // T1 in S2?
            result = store1.getConstraintCollectionByTarget(dummyTable);
            assertEquals(1, result.size());
            for (Object o : result) {
                assertTrue(store1.getConstraintCollections().contains(o));
            }
            // C1 in S2?
            result = store1.getConstraintCollectionByTarget(dummyCol);
            assertEquals(1, result.size());
            for (Object o : result) {
                assertTrue(store1.getConstraintCollections().contains(o));
            }

            // S1 not in S2?
            result = store1.getConstraintCollectionByTarget(dummySchema2);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
            // T1 not in S2?
            result = store1.getConstraintCollectionByTarget(dummyTable2);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
            // C1 not in S2?
            result = store1.getConstraintCollectionByTarget(dummyCol2);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
        }

        // XY in T2?
        {
            final MetadataStore store1 = new DefaultMetadataStore();
            final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "schema1", null, mock(Location.class));

            store1.getSchemas().add(dummySchema);
            Table dummyTable = dummySchema.addTable(store1, "table1", null, mock(Location.class));
            ConstraintCollection<InclusionDependency> constraintCollection =
                    store1.createConstraintCollection(null, InclusionDependency.class, dummyTable);
            Column dummyCol = dummyTable.addColumn(store1, "col1", null, 1);

            final Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, "schema2", null, mock(Location.class));
            store1.getSchemas().add(dummySchema2);
            Table dummyTable2 = dummySchema2.addTable(store1, "table2", null, mock(Location.class));
            Column dummyCol2 = dummyTable2.addColumn(store1, "col2", null, 1);

            Collection<ConstraintCollection<?>> result;

            // S1 in T2?
            result = store1.getConstraintCollectionByTarget(dummySchema);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
            // T1 in T2?
            result = store1.getConstraintCollectionByTarget(dummyTable);
            assertEquals(1, result.size());
            for (Object o : result) {
                assertTrue(store1.getConstraintCollections().contains(o));
            }
            // C1 in T2?
            result = store1.getConstraintCollectionByTarget(dummyCol);
            assertEquals(1, result.size());
            for (Object o : result) {
                assertTrue(store1.getConstraintCollections().contains(o));
            }

            // T1 not in S2?
            result = store1.getConstraintCollectionByTarget(dummyTable2);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
            // C1 not in S2?
            result = store1.getConstraintCollectionByTarget(dummyCol2);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
        }

        // XY in C2?
        {
            final MetadataStore store1 = new DefaultMetadataStore();
            final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "schema1", null, mock(Location.class));

            store1.getSchemas().add(dummySchema);
            Table dummyTable = dummySchema.addTable(store1, "table1", null, mock(Location.class));
            Column dummyCol = dummyTable.addColumn(store1, "col1", null, 1);
            ConstraintCollection<InclusionDependency> constraintCollection =
                    store1.createConstraintCollection(null, InclusionDependency.class, dummyCol);

            final Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, "schema2", null, mock(Location.class));
            store1.getSchemas().add(dummySchema2);
            Table dummyTable2 = dummySchema2.addTable(store1, "table2", null, mock(Location.class));
            Column dummyCol2 = dummyTable2.addColumn(store1, "col2", null, 1);

            Collection<ConstraintCollection<?>> result;

            // S1 in C2?
            result = store1.getConstraintCollectionByTarget(dummySchema);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
            // T1 in C2?
            result = store1.getConstraintCollectionByTarget(dummyTable);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
            // C1 in C2?
            result = store1.getConstraintCollectionByTarget(dummyCol);
            assertEquals(1, result.size());
            for (Object o : result) {
                assertTrue(store1.getConstraintCollections().contains(o));
            }

            // C1 not in C2?
            result = store1.getConstraintCollectionByTarget(dummyCol2);
            assertEquals(0, result.size());
            for (Object o : result) {
                Assert.assertFalse(store1.getConstraintCollections().contains(o));
            }
        }
    }

    @Test
    public void testGetConstraintCollectionByType() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "schema1", null, mock(Location.class));

        store1.getSchemas().add(dummySchema);
        store1.createConstraintCollection(null, InclusionDependency.class, dummySchema);
        {
            Collection<ConstraintCollection<InclusionDependency>> result;
            result = store1.getConstraintCollectionByConstraintType(InclusionDependency.class);
            assertEquals(1, result.size());
        }
        {
            Collection<ConstraintCollection<UniqueColumnCombination>> result;
            result = store1.getConstraintCollectionByConstraintType(UniqueColumnCombination.class);
            assertEquals(0, result.size());
        }
    }

    @Test
    public void testGetConstraintCollectionByTypeAndTarget() {
        // XY in T2?
        {
            final MetadataStore store1 = new DefaultMetadataStore();
            final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "schema1", null, mock(Location.class));

            store1.getSchemas().add(dummySchema);
            Table dummyTable = dummySchema.addTable(store1, "table1", null, mock(Location.class));
            ConstraintCollection<InclusionDependency> constraintCollection =
                    store1.createConstraintCollection(null, InclusionDependency.class, dummyTable);
            Column dummyCol = dummyTable.addColumn(store1, "col1", null, 1);

            final Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, "schema2", null, mock(Location.class));
            store1.getSchemas().add(dummySchema2);
            Table dummyTable2 = dummySchema2.addTable(store1, "table2", null, mock(Location.class));
            Column dummyCol2 = dummyTable2.addColumn(store1, "col2", null, 1);

            // Here having the CORRECT constraint type...
            {
                Collection<ConstraintCollection<InclusionDependency>> result;

                // S1 in T2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(InclusionDependency.class, dummySchema);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
                // T1 in T2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(InclusionDependency.class, dummyTable);
                assertEquals(1, result.size());
                for (Object o : result) {
                    assertTrue(store1.getConstraintCollections().contains(o));
                }
                // C1 in T2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(InclusionDependency.class, dummyCol);
                assertEquals(1, result.size());
                for (Object o : result) {
                    assertTrue(store1.getConstraintCollections().contains(o));
                }

                // T1 not in S2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(InclusionDependency.class, dummyTable2);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
                // C1 not in S2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(InclusionDependency.class, dummyCol2);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
            }
            // Here having the INCORRECT constraint type...
            {
                Collection<ConstraintCollection<UniqueColumnCombination>> result;

                // S1 in T2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(UniqueColumnCombination.class, dummySchema);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
                // T1 in T2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(UniqueColumnCombination.class, dummyTable);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
                // C1 in T2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(UniqueColumnCombination.class, dummyCol);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }

                // T1 not in S2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(UniqueColumnCombination.class, dummyTable2);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
                // C1 not in S2?
                result = store1.getConstraintCollectionByConstraintTypeAndScope(UniqueColumnCombination.class, dummyCol2);
                assertEquals(0, result.size());
                for (Object o : result) {
                    Assert.assertFalse(store1.getConstraintCollections().contains(o));
                }
            }
        }
    }

    @Test
    public void testGetConstraintCollectionByUserDefinedId() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Schema dummySchema = store1.addSchema("schema1", null, mock(Location.class));

        ConstraintCollection<InclusionDependency> cc = store1.createConstraintCollection("my-inds", null, null, InclusionDependency.class, dummySchema);
        {
            ConstraintCollection<InclusionDependency> loadedCC = store1.getConstraintCollection("my-inds");
            assertEquals(cc.getId(), loadedCC.getId());
        }
        {
            ConstraintCollection<UniqueColumnCombination> loadedCC = store1.getConstraintCollection("my-uccs");
            assertNull(loadedCC);
        }
    }

    @Test(expected = IdAlreadyInUseException.class)
    public void testDuplicateUserDefinedIdsForConstraintCollectionsFails() {
        final MetadataStore store1 = new DefaultMetadataStore();
        final Schema dummySchema = store1.addSchema("schema1", null, mock(Location.class));

        store1.createConstraintCollection("my-inds", null, null, InclusionDependency.class, dummySchema);
        store1.createConstraintCollection("my-inds", null, null, InclusionDependency.class, dummySchema);

    }
}
