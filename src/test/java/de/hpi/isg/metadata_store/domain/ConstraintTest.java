package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.*;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class ConstraintTest {

    @Test
    public void testTypeConstraint() {

	MetadataStore store1 = new DefaultMetadataStore(1, "test");

	Column dummyColumn = DefaultColumn.buildAndRegister(store1, 2, "dummyColumn1", new IndexedLocation(0, null));

	Constraint dummyTypeContraint1 = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));
	Constraint dummyTypeContraint2 = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddingConstraintsToUnmodifiableConstraintCollectionFails() {

	MetadataStore store1 = new DefaultMetadataStore(1, "test");

	Column dummyColumn = DefaultColumn.buildAndRegister(store1, 2, "dummyColumn1", new IndexedLocation(0, null));

	Constraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store1.getConstraints().add(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

	MetadataStore store2 = new DefaultMetadataStore(1, "test");

	Column dummyColumn = DefaultColumn.buildAndRegister(new Observer() {

	    @Override
	    public void update(Object message) {
		// TODO Auto-generated method stub

	    }
	}, 2, "dummyColumn2", new IndexedLocation(0, null));

	Constraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store2.addConstraint(dummyTypeContraint);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

	MetadataStore store3 = new DefaultMetadataStore(1, "test");

	Column dummyColumn = DefaultColumn.buildAndRegister(store3, 2, "dummyColumn3 ", new IndexedLocation(0, null));

	store3.getSchemas().add(
		DefaultSchema.buildAndRegister(store3, 3, "dummySchema", null).addTable(
			DefaultTable.buildAndRegister(store3, 4, "dummyTable", null).addColumn(dummyColumn)));

	Constraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store3.addConstraint(dummyTypeContraint);
    }

}
