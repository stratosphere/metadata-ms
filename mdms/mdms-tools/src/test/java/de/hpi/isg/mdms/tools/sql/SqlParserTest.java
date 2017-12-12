package de.hpi.isg.mdms.tools.sql;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

    @Test
    public void testParseForeignKeyDefinitions() {
        String inputFile = Thread.currentThread().getContextClassLoader().getResource("foreign-keys.sql").getFile();
        Collection<ForeignKeyDefinition> primaryKeyDefinitions = SQLParser.parseForeignKeys(inputFile);

        Assert.assertEquals(
                new HashSet<>(Arrays.asList(
                        new ForeignKeyDefinition(
                                "dep1", Collections.singletonList("that_id"),
                                "ref1", Collections.singletonList("id")
                        ),
                        new ForeignKeyDefinition(
                                "dep2", Collections.singletonList("that_id"),
                                "ref2", Collections.singletonList("id")
                        ),

                        new ForeignKeyDefinition(
                                "dep3", Arrays.asList("first_name", "last_name"),
                                "ref3", Arrays.asList("fn", "ln")
                        ),

                        new ForeignKeyDefinition(
                                "dep4", Collections.singletonList("that_oid"),
                                "ref4", Collections.singletonList("oid")
                        ),

                        new ForeignKeyDefinition(
                                "dep5", Arrays.asList("first_name", "last_name"),
                                "ref5", Arrays.asList("fn", "ln")
                        )
                )),
                new HashSet<>(primaryKeyDefinitions)
        );
    }

    @Test
    public void testParseDataTypes() {
        String inputFile = Thread.currentThread().getContextClassLoader().getResource("test-schema.sql").getFile();
        Collection<DataTypeDefinition> dataTypeDefinitions = SQLParser.parseDataTypes(inputFile);

        Assert.assertEquals(
                new HashSet<>(Arrays.asList(
                        new DataTypeDefinition("TABLE1", "NAME", "VARCHAR(255)"),
                        new DataTypeDefinition("TABLE1", "FIRST_NAME", "VARCHAR(255)"),
                        new DataTypeDefinition("TABLE1", "PROFESSION", "VARCHAR(50)"),
                        new DataTypeDefinition("TABLE2", "PROFESSION", "VARCHAR(255)"),
                        new DataTypeDefinition("TABLE2", "RECOGNITION", "VARCHAR(255)"),
                        new DataTypeDefinition("TABLE2", "COMMENTS", "TEXT")
                )),
                new HashSet<>(dataTypeDefinitions)
        );
    }

    @Test
    public void testParseNotNullConstraints() {
        String inputFile = Thread.currentThread().getContextClassLoader().getResource("test-schema.sql").getFile();
        Collection<NotNullDefinition> notNullDefinitions = SQLParser.parseNotNullConstraints(inputFile);

        Assert.assertEquals(
                new HashSet<>(Collections.singletonList(new NotNullDefinition("TABLE1", "PROFESSION"))),
                new HashSet<>(notNullDefinitions)
        );
    }

}
