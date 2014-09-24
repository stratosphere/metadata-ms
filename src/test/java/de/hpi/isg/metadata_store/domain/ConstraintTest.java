package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
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

	final Column dummyColumn = DefaultColumn.buildAndRegister(store1, 2, "dummyColumn1", new IndexedLocation(0,
		null));

	final Constraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store1.getConstraints().add(dummyTypeContraint);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnSchemasFails() {

	final MetadataStore store1 = new DefaultMetadataStore();

	final Schema dummySchema = DefaultSchema.buildAndRegister(store1, 2, "dummySchema", new Location() {
	});

	new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(dummySchema));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnTablesFails() {

	final MetadataStore store1 = new DefaultMetadataStore();

	final Table dummyTable = DefaultTable.buildAndRegister(store1, 2, "dummySchema", new Location() {
	});

	new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(dummyTable));

    }

    @Test
    public void testTypeConstraint() {

	final MetadataStore store1 = new DefaultMetadataStore();

	final Column dummyColumn = DefaultColumn.buildAndRegister(store1, 2, "dummyColumn1", new IndexedLocation(0,
		null));

	final Constraint dummyTypeContraint1 = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));
	final Constraint dummyTypeContraint2 = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

	final MetadataStore store3 = new DefaultMetadataStore();

	final Column dummyColumn = DefaultColumn.buildAndRegister(store3, 2, "dummyColumn3 ", new IndexedLocation(0,
		null));

	store3.getSchemas().add(
		DefaultSchema.buildAndRegister(store3, 3, "dummySchema", null).addTable(
			DefaultTable.buildAndRegister(store3, 4, "dummyTable", null).addColumn(dummyColumn)));

	final Constraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store3.addConstraint(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

	final MetadataStore store2 = new DefaultMetadataStore();

	final Column dummyColumn = DefaultColumn.buildAndRegister(new Observer() {

	    @Override
	    public void update(Object message) {
		// TODO Auto-generated method stub

	    }
	}, 2, "dummyColumn2", new IndexedLocation(0, null));

	final Constraint dummyTypeContraint = new TypeConstraint(3, "dummyTypeConstraint", new SingleTargetReference(
		dummyColumn));

	store2.addConstraint(dummyTypeContraint);
    }

}
