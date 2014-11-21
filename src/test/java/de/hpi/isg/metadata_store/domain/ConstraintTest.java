package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.constraints.impl.DistinctValueCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.TupleCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class ConstraintTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testAddingConstraintsToUnmodifiableConstraintCollectionFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(store1, mock(Table.class), "dummyColumn1",
                mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(
                dummyColumn),
                mock(ConstraintCollection.class), TYPES.STRING);

        store1.getConstraints().add(dummyTypeContraint);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnSchemasFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "dummySchema", mock(Location.class));

        TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(dummySchema),
                mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnTablesFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, mock(Schema.class), "dummySchema",
                mock(Location.class));

        TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(dummyTable),
                mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintWithTakenIdFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, mock(Schema.class), "dummySchema",
                mock(Location.class));

        TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(dummyTable),
                mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Test
    public void testTypeConstraint() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class),
                "dummyColumn1", mock(Location.class));

        final ConstraintCollection cC = mock(ConstraintCollection.class);
        final Constraint dummyTypeContraint1 = TypeConstraint.buildAndAddToCollection(1,
                new SingleTargetReference(dummyColumn), cC, TYPES.STRING);
        final Constraint dummyTypeContraint2 = TypeConstraint.buildAndAddToCollection(1,
                new SingleTargetReference(dummyColumn), cC, TYPES.STRING);

        assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test
    public void testTupleCount() {

        final Table dummyTable = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class),
                "dummyTable", new DefaultLocation());

        final ConstraintCollection cC = mock(ConstraintCollection.class);
        final Constraint tupleCount1 = TupleCount.buildAndAddToCollection(1,
                new TupleCount.Reference(dummyTable), cC, 1);
        final Constraint tupleCount2 = TupleCount.build(1,
                new TupleCount.Reference(dummyTable), cC, 1);

        assertEquals(tupleCount1, tupleCount2);
    }

    @Test
    public void testDistinctValueCount() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class),
                "dummyColumn1", mock(Location.class));

        final ConstraintCollection cC = mock(ConstraintCollection.class);
        final Constraint distinctValueCount1 = DistinctValueCount.buildAndAddToCollection(1,
                new SingleTargetReference(dummyColumn), cC, 1);
        final Constraint distinctValueCount = DistinctValueCount.buildAndAddToCollection(1,
                new SingleTargetReference(dummyColumn), cC, 1);

        assertEquals(distinctValueCount1, distinctValueCount);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

        final MetadataStore store = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(store, mock(Table.class), "dummyColumn3 ",
                mock(Location.class));

        store.getSchemas().add(
                DefaultSchema.buildAndRegister(store, "dummySchema", null).addTable(
                        DefaultTable.buildAndRegister(store, mock(Schema.class), "dummyTable", null).addColumn(
                                dummyColumn)));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(
                dummyColumn),
                mock(ConstraintCollection.class), TYPES.STRING);

        store.addConstraint(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

        final MetadataStore store2 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(mock(Observer.class), mock(Table.class),
                "dummyColumn2", mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(1, new SingleTargetReference(
                dummyColumn),
                mock(ConstraintCollection.class), TYPES.STRING);

        store2.addConstraint(dummyTypeContraint);
    }

}
