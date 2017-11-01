package de.hpi.isg.mdms.tools.apps;

import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collection;

/**
 * Test suite for the {@link MetanomeStatisticsImportApp}.
 */
public class MetanomeStatisticsImportAppTest {

    @Test
    public void testImportTestFile() throws Exception {
        DefaultMetadataStore metadataStore = new DefaultMetadataStore();
        Schema schema = metadataStore.addSchema("TPC-H", "", new DefaultLocation());
        Table table = schema.addTable(metadataStore, "customer.csv", "", new DefaultLocation());
        Column column1 = table.addColumn(metadataStore, "column1", "", 0);
        Column column2 = table.addColumn(metadataStore, "column2", "", 1);
        Column column3 = table.addColumn(metadataStore, "column3", "", 2);
        Column column4 = table.addColumn(metadataStore, "column4", "", 3);
        Column column5 = table.addColumn(metadataStore, "column5", "", 4);
        Column column6 = table.addColumn(metadataStore, "column6", "", 5);
        Column column7 = table.addColumn(metadataStore, "column7", "", 6);
        Column column8 = table.addColumn(metadataStore, "column8", "", 7);

        File inputFile = new File(Thread.currentThread().getContextClassLoader().getResource("metanome-statistics.json").toURI());

        MetanomeStatisticsImportApp.fromParameters(
                metadataStore,
                inputFile.getParentFile().getAbsolutePath(),
                ".*-statistics\\.json",
                "TPC-H",
                null,
                null
        );

        // Check that we have all tuple counts.
        Collection<ConstraintCollection<TupleCount>> tupleCountConstraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        TupleCount.class,
                        schema
                );
        Assert.assertEquals(1, tupleCountConstraintCollections.size());
        ConstraintCollection<TupleCount> tupleCountConstraintCollection = tupleCountConstraintCollections.iterator().next();
        Assert.assertEquals(1, tupleCountConstraintCollection.getConstraints().size());
        TupleCount tupleCount = tupleCountConstraintCollection.getConstraints().stream()
                .filter(tc -> tc.getTableId() == table.getId())
                .findAny()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals(26, tupleCount.getNumTuples());

        // Check that we have all the type constraints.
        Collection<ConstraintCollection<TypeConstraint>> typeConstraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        TypeConstraint.class,
                        schema
                );
        Assert.assertEquals(1, typeConstraintCollections.size());
        ConstraintCollection<TypeConstraint> typeConstraintCollection = typeConstraintCollections.iterator().next();
        Assert.assertEquals(8, typeConstraintCollection.getConstraints().size());
        TypeConstraint typeConstraint1 = typeConstraintCollection.getConstraints().stream()
                .filter(typeConstraint -> typeConstraint.getColumnId() == column1.getId())
                .findAny()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals("INT", typeConstraint1.getType());

        // Check that we have all the distinct values.
        Collection<ConstraintCollection<DistinctValueCount>> distinctValueConstraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        DistinctValueCount.class,
                        schema
                );
        Assert.assertEquals(1, typeConstraintCollections.size());
        ConstraintCollection<DistinctValueCount> distinctValueConstraintCollection = distinctValueConstraintCollections.iterator().next();
        Assert.assertEquals(8, distinctValueConstraintCollection.getConstraints().size());
        DistinctValueCount dvCount1 = distinctValueConstraintCollection.getConstraints().stream()
                .filter(typeConstraint -> typeConstraint.getColumnId() == column1.getId())
                .findAny()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals(150000, dvCount1.getNumDistinctValues());

        // Check that we have all the general statistics.
        Collection<ConstraintCollection<ColumnStatistics>> columnStatisticsCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(ColumnStatistics.class, schema);
        Assert.assertEquals(1, columnStatisticsCollections.size());
        ConstraintCollection<ColumnStatistics> statisticsConstraintCollection = columnStatisticsCollections.iterator().next();
        Assert.assertEquals(8, statisticsConstraintCollection.getConstraints().size());
        ColumnStatistics columnStatistics = statisticsConstraintCollection.getConstraints().stream()
                .filter(cs -> cs.getColumnId() == column8.getId())
                .findFirst()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals(9, columnStatistics.getTopKFrequentValues().size());
        Assert.assertEquals(
                " Tiresias sleep furiously express deposits. qu",
                columnStatistics.getTopKFrequentValues().get(8).getValue()
        );
        Assert.assertEquals(1, columnStatistics.getTopKFrequentValues().get(8).getNumOccurrences());
        Assert.assertEquals(149968, columnStatistics.getNumDistinctValues());
        Assert.assertEquals(1, columnStatistics.getFillStatus(), 0d);
        Assert.assertEquals(0, columnStatistics.getNumNulls());
        Assert.assertTrue(Double.isNaN(columnStatistics.getEntropy()));
        columnStatistics = statisticsConstraintCollection.getConstraints().stream()
                .filter(cs -> cs.getColumnId() == column4.getId())
                .findFirst()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals(2d, columnStatistics.getEntropy(), 0);

        // Check that we have all the text constraints.
        Collection<ConstraintCollection<TextColumnStatistics>> textConstraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        TextColumnStatistics.class,
                        schema
                );
        Assert.assertEquals(1, textConstraintCollections.size());
        ConstraintCollection<TextColumnStatistics> textConstraintCollection = textConstraintCollections.iterator().next();
        Assert.assertEquals(5, textConstraintCollection.getConstraints().size());
        TextColumnStatistics textColumnStatistics = textConstraintCollection.getConstraints().stream()
                .filter(textColumnStatistics1 -> textColumnStatistics1.getColumnId() == column2.getId())
                .findAny()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals("Customer#000000001", textColumnStatistics.getMinValue());
        Assert.assertEquals("Customer#000150000", textColumnStatistics.getMaxValue());
        Assert.assertEquals("Customer#000150000", textColumnStatistics.getShortestValue());
        Assert.assertEquals("Customer#000150000", textColumnStatistics.getLongestValue());
        Assert.assertNull(textColumnStatistics.getSubtype());

        // Check that we have all the number constraints.
        Collection<ConstraintCollection<NumberColumnStatistics>> numberConstraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        NumberColumnStatistics.class,
                        schema
                );
        Assert.assertEquals(1, numberConstraintCollections.size());
        ConstraintCollection<NumberColumnStatistics> numberConstraintCollection = numberConstraintCollections.iterator().next();
        Assert.assertEquals(3, numberConstraintCollection.getConstraints().size());
        NumberColumnStatistics numberColumnStatistics = numberConstraintCollection.getConstraints().stream()
                .filter(ncs -> ncs.getColumnId() == column4.getId())
                .findAny()
                .orElseThrow(AssertionError::new);
        Assert.assertEquals(0.0, numberColumnStatistics.getMinValue(), 0d);
        Assert.assertEquals(24.0, numberColumnStatistics.getMaxValue(), 0d);
        Assert.assertEquals(12.0067, numberColumnStatistics.getAverage(), 1e-5);

    }

}