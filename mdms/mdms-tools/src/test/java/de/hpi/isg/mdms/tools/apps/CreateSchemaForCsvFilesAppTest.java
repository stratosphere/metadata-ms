package de.hpi.isg.mdms.tools.apps;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test suite for {@link CreateSchemaForCsvFilesApp}.
 */
public class CreateSchemaForCsvFilesAppTest {

    private static File getTestResource(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(name).toURI());
    }

    @Test
    public void testImportViaFromParameters() throws Exception {
        MetadataStore metadataStore = new DefaultMetadataStore();

        File testFolder = getTestResource("test-schema");

        CreateSchemaForCsvFilesApp.fromParameters(
                metadataStore,
                testFolder.getAbsolutePath(),
                "test-schema",
                ";",
                "\"",
                true
        );

        Schema schema = metadataStore.getSchemaByName("test-schema");
        Assert.assertNotNull("test-schema");
        Assert.assertEquals(2, schema.getTables().size());
        Table table1 = metadataStore.getTableByName("test-schema.table1");
        Assert.assertNotNull(table1);
        Assert.assertEquals(3, table1.getColumns().size());
        Column table1Comments = metadataStore.getColumnByName("test-schema.table2.dat.Comments");
        Assert.assertNotNull(table1Comments);
    }


    @Test
    public void testImportWithSqlFile() throws Exception {
        MetadataStore metadataStore = new DefaultMetadataStore();

        File testFolder = getTestResource("test-schema");

        CreateSchemaForCsvFilesApp.fromParameters(
                metadataStore,
                testFolder.getAbsolutePath(),
                "test-schema",
                ";",
                "\"",
                false,
                getTestResource("test-schema.sql").getAbsolutePath()
        );

        Schema schema = metadataStore.getSchemaByName("test-schema");
        Assert.assertNotNull("test-schema");
        Assert.assertEquals(2, schema.getTables().size());
        Table table1 = metadataStore.getTableByName("test-schema.table1");
        Assert.assertNotNull(table1);
        Assert.assertEquals(3, table1.getColumns().size());
        List<Column> columns = new ArrayList<>(table1.getColumns());
        columns.sort(Comparator.comparingInt(c -> metadataStore.getIdUtils().getLocalColumnId(c.getId())));
        Assert.assertEquals("NAME", columns.get(0).getName());
        Assert.assertEquals("FIRST_NAME", columns.get(1).getName());
        Assert.assertEquals("PROFESSION", columns.get(2).getName());

        Table table2 = metadataStore.getTableByName("test-schema.table2.dat");
        Assert.assertNotNull(table2);
        Assert.assertEquals(3, table2.getColumns().size());
        columns = new ArrayList<>(table2.getColumns());
        columns.sort(Comparator.comparingInt(c -> metadataStore.getIdUtils().getLocalColumnId(c.getId())));
        Assert.assertEquals("PROFESSION", columns.get(0).getName());
        Assert.assertEquals("RECOGNITION", columns.get(1).getName());
        Assert.assertEquals("COMMENTS", columns.get(2).getName());
    }

    @Test
    public void testImportToSqliteViaFromParameters() throws Exception {
        File dbFile = File.createTempFile("test", "db");
        dbFile.deleteOnExit();
        SQLiteInterface sqliteInterface = SQLiteInterface.createForFile(dbFile);
        MetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(sqliteInterface);

        File testFolder = getTestResource("test-schema");

        CreateSchemaForCsvFilesApp.fromParameters(
            metadataStore,
                testFolder.getAbsolutePath(),
                "test-schema",
                ";",
                "\"",
                true
        );

        Schema schema = metadataStore.getSchemaByName("test-schema");
        Assert.assertNotNull("test-schema");
        Assert.assertEquals(2, schema.getTables().size());
        Table table1 = metadataStore.getTableByName("test-schema.table1");
        Assert.assertNotNull(table1);
        Assert.assertEquals(3, table1.getColumns().size());
        Column table1Comments = metadataStore.getColumnByName("test-schema.table2.dat.Comments");
        Assert.assertNotNull(table1Comments);
    }

}