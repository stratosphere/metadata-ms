package de.hpi.isg.metadata_store.in_memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.factories.MetadataStoreFactory;
import de.hpi.isg.metadata_store.domain.impl.DefaultConstraintCollection;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;
import de.hpi.isg.metadata_store.exceptions.MetadataStoreNotFoundException;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

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
        final Schema schema1 = DefaultSchema.buildAndRegister(store1, "pdb", mock(Location.class));
        store1.addSchema(schema1);
        assertTrue(store1.getSchemas().contains(schema1));
        assertTrue(store1.getAllTargets().contains(schema1));
    }

    @Test
    public void testConstructingAComplexSchema() {
        final MetadataStore metadataStore = new DefaultMetadataStore();
        for (int schemaNumber = 0; schemaNumber <= Math.min(3, metadataStore.getIdUtils().getMaxSchemaNumber()); schemaNumber++) {
            final Schema schema = metadataStore.addSchema(String.format("schema-%03d", schemaNumber), null);
            for (int tableNumber = 0; tableNumber < 100 * loadFactorForCreateComplexSchemaTest; tableNumber++) {
                final Table table = schema.addTable(metadataStore, String.format("table-%03d", schemaNumber), null);
                for (int columnNumber = 0; columnNumber < 10 * loadFactorForCreateComplexSchemaTest; columnNumber++) {
                    table.addColumn(metadataStore, String.format("column-%03d", columnNumber), columnNumber);
                }
            }
        }
        
        System.out.println("Adding inclusion dependencies.");
        final Random random = new Random();
        for (final Schema schema : metadataStore.getSchemas()) {
            ConstraintCollection constraintCollection = metadataStore.createConstraintCollection(schema);
            int numInclusionDependencies = 0;
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
                                                reference, mock(ConstraintCollection.class));
                                constraintCollection.add(inclusionDependency);
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

        MetadataStoreFactory.loadDefaultMetadataStore(file);
    }

    @Test
    public void testGetOrCreateOfExisting() {
        final File file = new File(this.dir, "filledStore.ms");
        // setup store
        final DefaultMetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "PDB", new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, dummySchema, "dummyTable", dummyTableLocation);

        final Column dummyColumn = DefaultColumn.buildAndRegister(store1, dummyTable, "dummyColumn",
                new DefaultLocation());

        final ConstraintCollection cC = store1.createConstraintCollection(dummySchema);
        final Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn), cC, TYPES.STRING);

        store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn)));

        try {
            store1.save(file.getAbsolutePath());
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        MetadataStore store2 = null;
        try {
            store2 = MetadataStoreFactory.loadOrCreateAndSaveDefaultMetadataStore(file);
        } catch (MetadataStoreNotFoundException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertEquals(store1, store2);
    }

    @Test
    public void testGetOrCreateOfNotExisting() {
        final File file = new File(this.dir, "notExisting.ms");

        MetadataStore store2 = null;
        try {
            store2 = MetadataStoreFactory.loadOrCreateAndSaveDefaultMetadataStore(file);
        } catch (MetadataStoreNotFoundException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertEquals(store2, new DefaultMetadataStore());
    }

    @Test
    public void testRetrievingOfSchemaByName() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", mock(Location.class));
        store1.getSchemas().add(dummySchema1);

        assertEquals(store1.getSchemaByName("PDB"), dummySchema1);
    }

    @Test
    public void testConstaintCollections() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", mock(Location.class));
        store1.getSchemas().add(dummySchema1);
        Column col = dummySchema1.addTable(store1, "table1", mock(Location.class)).addColumn(store1, "foo", 1);
        final Set<?> scope = Collections.singleton(dummySchema1);
        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(col),
                mock(ConstraintCollection.class),
                TYPES.STRING);
        final Set<Constraint> constraints = Collections.singleton(dummyTypeContraint);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(dummySchema1);

        assertTrue(store1.getConstraintCollections().contains(constraintCollection));
    }

    @Test(expected = NameAmbigousException.class)
    public void testRetrievingOfSchemaByNameWithAmbigousNameFails() {
        // setup store
        final MetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", mock(Location.class));
        store1.getSchemas().add(dummySchema1);

        final Schema dummySchema2 = DefaultSchema.buildAndRegister(store1, "PDB", mock(Location.class));
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
            store2 = MetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertEquals(store1, store2);
    }

    @Test
    public void testStoringOfFilledMetadataStore() {
        final File file = new File(this.dir, "filledStore.ms");
        // setup store
        final DefaultMetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "PDB", new DefaultLocation());

        final DefaultLocation dummyTableLocation = new DefaultLocation();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, dummySchema, "dummyTable", dummyTableLocation);

        final Column dummyColumn = DefaultColumn.buildAndRegister(store1, dummyTable, "dummyColumn",
                new DefaultLocation());

        final ConstraintCollection dummyConstraintCollection = store1.createConstraintCollection(dummySchema);

        final Constraint dummyContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn), dummyConstraintCollection, TYPES.STRING);

        // XXX Do we need this line?
        store1.getSchemas().add(dummySchema.addTable(dummyTable.addColumn(dummyColumn)));

        try {
            store1.save(file.getAbsolutePath());
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // retrieve store
        MetadataStore store2 = null;

        try {
            store2 = MetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertEquals(dummySchema, store2.getSchemas().iterator().next());

        Constraint cc1 = store1.getConstraintCollections().iterator().next().getConstraints().iterator().next();
        Constraint cc2 = store2.getConstraintCollections().iterator().next().getConstraints().iterator().next();

        cc1.equals(cc2);

        assertEquals(cc1, cc2);
        assertEquals(store1, store2);
    }

    @Test
    public void testStoringOfFilledMetadataStore2() {
        final File file = new File(this.dir, "filledStore.ms");
        // setup store
        final DefaultMetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "PDB", new DefaultLocation());
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
            store2 = MetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertEquals(store1, store2);

        final Schema schema = store2.getSchemas().iterator().next();

        assertEquals(dummySchema, schema);
    }

    @Test
    public void testStoringOfFilledMetadataStore3() {
        final File file = new File(this.dir, "filledStore.ms");
        // setup store
        final DefaultMetadataStore store1 = new DefaultMetadataStore();
        // setup schema
        final Schema dummySchema1 = DefaultSchema.buildAndRegister(store1, "PDB", new DefaultLocation())
                .addTable(DefaultTable.buildAndRegister(store1, mock(Schema.class), "foo", null));
        store1.getSchemas().add(dummySchema1);

        try {
            store1.save(file.getAbsolutePath());
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // retrieve store
        MetadataStore store2 = null;

        try {
            store2 = MetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        final Collection<Target> allTargets1 = store1.getAllTargets();
        final Collection<Target> allTargets2 = store2.getAllTargets();
        assertTrue(allTargets1.contains(dummySchema1));
        assertTrue(allTargets2.contains(dummySchema1));

        assertEquals(store1, store2);
    }
}
