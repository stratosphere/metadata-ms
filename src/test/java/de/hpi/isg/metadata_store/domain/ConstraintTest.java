package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.*;

import java.util.Observable;
import java.util.Observer;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.common.MyObserver;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.impl.MetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.IColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.Column;
import de.hpi.isg.metadata_store.domain.targets.impl.Schema;
import de.hpi.isg.metadata_store.domain.targets.impl.Table;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class ConstraintTest {

    @Test
    public void testTypeConstraint() {

	IMetadataStore store1 = new MetadataStore(1, "test");

	IColumn dummyColumn = Column.buildAndRegister(store1, 2, "dummyColumn1", new IndexedLocation(0, null));

	IConstraint dummyTypeContraint1 = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));
	IConstraint dummyTypeContraint2 = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddingConstraintsToUnmodifiableConstraintCollectionFails() {

	IMetadataStore store1 = new MetadataStore(1, "test");

	IColumn dummyColumn = Column.buildAndRegister(store1, 2, "dummyColumn1", new IndexedLocation(0, null));

	IConstraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store1.getConstraints().add(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

	IMetadataStore store2 = new MetadataStore(1, "test");

	IColumn dummyColumn = Column.buildAndRegister(new MyObserver() {

	    @Override
	    public void update(Object message) {
		// TODO Auto-generated method stub

	    }
	}, 2, "dummyColumn2", new IndexedLocation(0, null));

	IConstraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store2.addConstraint(dummyTypeContraint);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

	IMetadataStore store3 = new MetadataStore(1, "test");

	IColumn dummyColumn = Column.buildAndRegister(store3, 2, "dummyColumn3 ", new IndexedLocation(0, null));

	store3.getSchemas().add(
		Schema.buildAndRegister(store3, 3, "dummySchema", null).addTable(
			Table.buildAndRegister(store3, 4, "dummyTable", null).addColumn(dummyColumn)));

	IConstraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store3.addConstraint(dummyTypeContraint);
    }

}
