package de.hpi.isg.mdms.dependencies;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Collection;

import de.hpi.isg.mdms.domain.Constraint;
import de.hpi.isg.mdms.domain.ConstraintCollection;
import de.hpi.isg.mdms.domain.Location;
import de.hpi.isg.mdms.domain.MetadataStore;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import de.hpi.isg.mdms.domain.common.Observer;
import de.hpi.isg.mdms.domain.constraints.impl.DistinctValueCount;
import de.hpi.isg.mdms.domain.constraints.impl.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.impl.TupleCount;
import de.hpi.isg.mdms.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.mdms.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.mdms.domain.constraints.impl.UniqueColumnCombination;
import de.hpi.isg.mdms.domain.impl.DefaultMetadataStore;
import de.hpi.isg.mdms.domain.impl.SingleTargetReference;
import de.hpi.isg.mdms.domain.location.impl.DefaultLocation;
import de.hpi.isg.mdms.domain.targets.Column;
import de.hpi.isg.mdms.domain.targets.Schema;
import de.hpi.isg.mdms.domain.targets.Table;
import de.hpi.isg.mdms.domain.targets.impl.DefaultColumn;
import de.hpi.isg.mdms.domain.targets.impl.DefaultSchema;
import de.hpi.isg.mdms.domain.targets.impl.DefaultTable;
import de.hpi.isg.mdms.exceptions.NotAllTargetsInStoreException;
import org.mockito.Mockito;

public class ConstraintTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testAddingConstraintsToUnmodifiableConstraintCollectionFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(store1, Mockito.mock(Table.class), "dummyColumn1", null,
                Mockito.mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()),
                Mockito.mock(ConstraintCollection.class), TYPES.STRING);

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null);
        ((Collection<Constraint>) constraintCollection.getConstraints()).add(dummyTypeContraint);
    }

    @Ignore("for this test to pass, the DefaultMetadataStore must implement meaningful IDs")
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnSchemasFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "dummySchema", null, Mockito.mock(Location.class));

        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(dummySchema.getId()),
                Mockito.mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Ignore("for this test to pass, the DefaultMetadataStore must implement meaningful IDs")
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnTablesFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, Mockito.mock(Schema.class), "dummySchema", null,
                Mockito.mock(Location.class));

        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(dummyTable.getId()),
                Mockito.mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Test
    public void testTypeConstraint() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));

        final ConstraintCollection cC = Mockito.mock(ConstraintCollection.class);
        final Constraint dummyTypeContraint1 = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn.getId()), cC, TYPES.STRING);
        final Constraint dummyTypeContraint2 = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn.getId()), cC, TYPES.STRING);

        Assert.assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test
    public void testTupleCount() {

        final Table dummyTable = DefaultTable.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Schema.class),
                "dummyTable", null, new DefaultLocation());

        final ConstraintCollection cC = Mockito.mock(ConstraintCollection.class);
        final TupleCount tupleCount1 = TupleCount.buildAndAddToCollection(
                new SingleTargetReference(dummyTable.getId()), cC, 1);
        final TupleCount tupleCount2 = TupleCount.build(
                new SingleTargetReference(dummyTable.getId()), cC, 1);

        assertEquals(tupleCount1.getNumTuples(), 1);

        assertEquals(tupleCount1, tupleCount2);
    }

    @Test
    public void testDistinctValueCount() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));

        final ConstraintCollection cC = Mockito.mock(ConstraintCollection.class);
        final DistinctValueCount distinctValueCount1 = DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn.getId()), cC, 1);
        final DistinctValueCount distinctValueCount2 = DistinctValueCount.build(
                new SingleTargetReference(dummyColumn.getId()), cC, 1);

        assertEquals(distinctValueCount2.getNumDistinctValues(), 1);

        assertEquals(distinctValueCount1, distinctValueCount2);
    }

    @Test
    public void testInclusionDependency() {

        final Column dummyColumn1 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));
        final Column dummyColumn2 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));

        final ConstraintCollection cC = Mockito.mock(ConstraintCollection.class);
        final InclusionDependency ind1 = InclusionDependency.build(new InclusionDependency.Reference(new Column[] {
                dummyColumn1,
                dummyColumn2 },
                new Column[] { dummyColumn2, dummyColumn1 }), cC);
        final InclusionDependency ind2 = InclusionDependency.build(new InclusionDependency.Reference(new Column[] {
                dummyColumn1,
                dummyColumn2 },
                new Column[] { dummyColumn2, dummyColumn1 }), cC);

        assertEquals(ind1.getArity(), 2);

        assertEquals(ind1, ind2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssymetricInclusionDependencyFails() {

        final Column dummyColumn1 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));
        final Column dummyColumn2 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));
        final Column dummyColumn3 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn3", null, Mockito.mock(Location.class));

        final ConstraintCollection cC = Mockito.mock(ConstraintCollection.class);
        @SuppressWarnings("unused")
        final InclusionDependency ind1 = InclusionDependency.build(new InclusionDependency.Reference(
                new Column[] { dummyColumn1 },
                new Column[] { dummyColumn2, dummyColumn3 }), cC);
    }

    @Test
    public void testUniqueColumnCombination() {

        final Column dummyColumn1 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));
        final Column dummyColumn2 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));

        final ConstraintCollection cC = Mockito.mock(ConstraintCollection.class);
        final UniqueColumnCombination ucc1 = UniqueColumnCombination.build(new UniqueColumnCombination.Reference(
                new int[] { dummyColumn1.getId(), dummyColumn2.getId() }), cC);
        final UniqueColumnCombination ucc2 = UniqueColumnCombination.build(new UniqueColumnCombination.Reference(
                new int[] { dummyColumn1.getId(), dummyColumn2.getId() }), cC);

        assertEquals(ucc1.getArity(), 2);

        assertEquals(ucc1, ucc2);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

        final MetadataStore store = new DefaultMetadataStore();

        final Schema dummySchema = DefaultSchema.buildAndRegister(store, "dummySchema", null, null);
        store.getSchemas().add(dummySchema);
        final Column dummyColumn = dummySchema.addTable(store, "dummyTable", "", Mockito.mock(Location.class)).addColumn(store,
                "dummyColumn3 ", "", 1);

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()),
                Mockito.mock(ConstraintCollection.class), TYPES.STRING);

        ConstraintCollection constraintCollection = store.createConstraintCollection(null);
        constraintCollection.add(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

        final MetadataStore store2 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(Mockito.mock(Observer.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()),
                Mockito.mock(ConstraintCollection.class), TYPES.STRING);

        ConstraintCollection constraintCollection = store2.createConstraintCollection(null);
        constraintCollection.add(dummyTypeContraint);
    }

}
