package de.hpi.isg.mdms.java.fk;

import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Test suite for the {@link SimpleForeignKeyDetector} class.
 */
public class SimpleForeignKeyDetectorTest {

    @Test
    public void testDetectDoesNotFail() {
        MetadataStore metadataStore = new DefaultMetadataStore();

        // Set up a schema.
        Schema schema = metadataStore.addSchema("schema", null, new DefaultLocation());
        for (String tableName : Arrays.asList("table1", "table2")) {
            Table table = schema.addTable(metadataStore, tableName, null, new DefaultLocation());
            for (int i = 0; i < 10; i++) {
                table.addColumn(metadataStore, String.format("column%d", i), null, i);
            }
        }

        // Set up some INDs.
        ConstraintCollection<InclusionDependency> indCC = metadataStore.createConstraintCollection(
                "inds", InclusionDependency.class, schema
        );
        indCC.add(new InclusionDependency(
                schema.getTargetByName("table2.column1").getId(),
                schema.getTargetByName("table1.column1").getId()
        ));
        indCC.add(new InclusionDependency(
                schema.getTargetByName("table2.column2").getId(),
                schema.getTargetByName("table1.column1").getId()
        ));
        indCC.add(new InclusionDependency(
                schema.getTargetByName("table2.column2").getId(),
                schema.getTargetByName("table1.column5").getId()
        ));
        indCC.add(new InclusionDependency(
                schema.getTargetByName("table2.column5").getId(),
                schema.getTargetByName("table1.column5").getId()
        ));

        // Set up some UCCs.
        ConstraintCollection<UniqueColumnCombination> uniqueCC = metadataStore.createConstraintCollection(
                "uccs", UniqueColumnCombination.class, schema
        );
        uniqueCC.add(new UniqueColumnCombination(new int[]{schema.getTargetByName("table1.column1").getId()}));
        uniqueCC.add(new UniqueColumnCombination(new int[]{schema.getTargetByName("table1.column2").getId()}));
        uniqueCC.add(new UniqueColumnCombination(new int[]{schema.getTargetByName("table1.column5").getId()}));

        // Set up tuple counts.
        ConstraintCollection<TupleCount> tupleCountCC = metadataStore.createConstraintCollection(
                "#tuples", TupleCount.class, schema
        );
        tupleCountCC.add(new TupleCount(schema.getTableByName("table1").getId(), 1_000_000));
        tupleCountCC.add(new TupleCount(schema.getTableByName("table2").getId(), 100_000));

        // Set up statistics.
        ConstraintCollection<ColumnStatistics> statsCC = metadataStore.createConstraintCollection(
                "stats", ColumnStatistics.class, schema
        );
        ConstraintCollection<TextColumnStatistics> textStatsCC = metadataStore.createConstraintCollection(
                "text stats", TextColumnStatistics.class, schema
        );
        for (String tableName : Arrays.asList("table1", "table2")) {
            Table table = schema.getTableByName(tableName);
            for (int i = 0; i < 10; i++) {
                Column column = table.getColumnByName(String.format("column%d", i));
                ColumnStatistics columnStatistics = new ColumnStatistics(column.getId());
                columnStatistics.setNumDistinctValues(-1);
                columnStatistics.setNumNulls(0);
                columnStatistics.setFillStatus(1);
                statsCC.add(columnStatistics);

                TextColumnStatistics textColumnStatistics = new TextColumnStatistics(column.getId());
                textColumnStatistics.setShortestValue("bla");
                textColumnStatistics.setLongestValue("blubb");
                textStatsCC.add(textColumnStatistics);
            }
        }

        List<SimpleForeignKeyDetector.ForeignKeyCandidate> fkCandidates = SimpleForeignKeyDetector.detect(
                indCC, uniqueCC, tupleCountCC, statsCC, textStatsCC, false
        );
        DependencyPrettyPrinter prettyPrinter = new DependencyPrettyPrinter(metadataStore);
        for (SimpleForeignKeyDetector.ForeignKeyCandidate fkCandidate : fkCandidates) {
            System.out.printf("* %s\n", fkCandidate.explain(prettyPrinter));
        }

    }

}