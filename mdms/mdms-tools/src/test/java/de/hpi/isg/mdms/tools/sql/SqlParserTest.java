package de.hpi.isg.mdms.tools.sql;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * This is a test suite for the {@link SQLParser}.
 */
public class SqlParserTest {

    @Test
    public void testParsePrimaryKeyDefinitions() {
        String inputFile = Thread.currentThread().getContextClassLoader().getResource("primary-keys.sql").getFile();
        Collection<PrimaryKeyDefinition> primaryKeyDefinitions = SQLParser.parsePrimaryKeys(inputFile);

        Assert.assertEquals(
                new HashSet<>(Arrays.asList(
                        new PrimaryKeyDefinition("table1", "id"),
                        new PrimaryKeyDefinition("table2", "id"),
                        new PrimaryKeyDefinition("table3", "first_name", "last_name"),
                        new PrimaryKeyDefinition("table4", "added_id"),
                        new PrimaryKeyDefinition("table5", "added_fn", "added_ln")
                )),
                new HashSet<>(primaryKeyDefinitions)
        );
    }

}
