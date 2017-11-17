package de.hpi.isg.mdms.java.schema_inference;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.TableSample;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Test suite for {@link ForeignKeys}.
 */
public class ForeignKeysTest {

    @Test
    public void testLongestSubstringLength() {
        Assert.assertEquals(0, ForeignKeys.longestSubstringLength("", "abc"));
        Assert.assertEquals(0, ForeignKeys.longestSubstringLength("abc", ""));
        Assert.assertEquals(3, ForeignKeys.longestSubstringLength("abc", "abcd"));
        Assert.assertEquals(3, ForeignKeys.longestSubstringLength("abcd", "abc"));
        Assert.assertEquals(6, ForeignKeys.longestSubstringLength("abababbad", "ababba"));
        Assert.assertEquals(5, ForeignKeys.longestSubstringLength("cababababac", "dababad"));
    }

    @Test
    public void testTestTextWilcoxonRankSum() {
        double p = ForeignKeys.testTextWilcoxonRankSum(
                new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e")),
                new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e"))
        );
        Assert.assertEquals(.5, p, .001);

        p = ForeignKeys.testTextWilcoxonRankSum(
                new ArrayList<>(Arrays.asList("a", "b", "c", "d")),
                new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e"))
        );
        Assert.assertEquals(8.0 / 20.0, p, .001);

        p = ForeignKeys.testTextWilcoxonRankSum(
                new ArrayList<>(Arrays.asList("b", "b", "c", "c")),
                new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e"))
        );
        Assert.assertEquals(8.0 / 20.0, p, .001);

        p = ForeignKeys.testTextWilcoxonRankSum(
                new ArrayList<>(Collections.singletonList("a")),
                new ArrayList<>(Arrays.asList("b", "c", "d", "e"))
        );
        Assert.assertEquals(0.0, p, .001);


        p = ForeignKeys.testTextWilcoxonRankSum(
                new ArrayList<>(Arrays.asList("f", "g")),
                new ArrayList<>(Arrays.asList("b", "c", "d", "e"))
        );
        Assert.assertEquals(1.0, p, .001);
    }

    @Test
    public void testDistributionalSimilarity() {
        MetadataStore store = new DefaultMetadataStore();
        Schema schema = store.addSchema("schema", null, null);
        Table table1 = schema.addTable(store, "table1", null, null);
        Column column1 = table1.addColumn(store, "col1", null, 0);
        Column column2 = table1.addColumn(store, "col2", null, 1);
        Table table2 = schema.addTable(store, "table2", null, null);
        Column column3 = table2.addColumn(store, "col3", null, 0);
        Column column4 = table2.addColumn(store, "col4", null, 1);

        TableSample tableSample1 = new TableSample(table1.getId(),
                new String[][]{
                        new String[]{"a", "0"},
                        new String[]{"a", "1"},
                        new String[]{"a", "2"},
                        new String[]{"c", "3"},
                        new String[]{"a", "4"},
                        new String[]{"a", "5"},
                        new String[]{"a", "6"},
                        new String[]{"e", "7"}
                }
        );
        TableSample tableSample2 = new TableSample(table2.getId(),
                new String[][]{
                        new String[]{"a", "10"},
                        new String[]{"b", "100"},
                        new String[]{"c", "200"},
                        new String[]{"d", "300"},
                        new String[]{"e", "400"},
                        new String[]{"f", "500"},
                        new String[]{"g", "600"},
                        new String[]{"h", "700"}
                }
        );

        double result1 = ForeignKeys.estimateDistributionDistance(
                column1.getId(),
                tableSample1,
                column1.getId(),
                tableSample1,
                store.getIdUtils()
        );

        double result2 = ForeignKeys.estimateDistributionDistance(
                column1.getId(),
                tableSample1,
                column3.getId(),
                tableSample2,
                store.getIdUtils()
        );

        double result3 = ForeignKeys.estimateDistributionDistance(
                column2.getId(),
                tableSample1,
                column4.getId(),
                tableSample2,
                store.getIdUtils()
        );

        Assert.assertEquals(0d, result1, 0d);
        Assert.assertTrue(result2 > 0d);
        Assert.assertTrue(result3 > 0d);
        Assert.assertTrue(result2 < result3);
    }

    @Test
    public void testEarthMoversDistanceCalculation() {
        // Using the example from http://www.mathcs.emory.edu/~cheung/Courses/323/Syllabus/Transportation/algorithm.html
        double emd = ForeignKeys.calculateEarthMoverDistance(
                new double[]{6, 9},
                new double[]{4, 6, 5},
                new double[][]{
                        new double[]{3, 5, 7},
                        new double[]{6, 4, 3}
                }
        );
        Assert.assertEquals(4 * 3 + 2 * 5 + 4 * 4 + 5 * 3, emd, 0.0001);
    }

    @Test
    public void testCreateTrainingSetOnRealWorldMetadataDoesNotFail() throws SQLException {
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("biosql.db").getFile());
        SQLiteInterface sqliteInterface = SQLiteInterface.createForFile(file);
        MetadataStore store = RDBMSMetadataStore.load(sqliteInterface);

        ForeignKeys.createTrainingSet(
                store,
                store.getConstraintCollection("sql-foreign-keys"),
                store.getConstraintCollection("metanome-inds"),
                store.getConstraintCollection("metanome-column-stats"),
                store.getConstraintCollection("metanome-text-stats"),
                store.getConstraintCollection("table-samples-100")
        );
    }

}