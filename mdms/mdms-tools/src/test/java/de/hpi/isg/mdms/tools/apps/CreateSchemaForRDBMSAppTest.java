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

/**
 * Test suite for {@link CreateSchemaForRDBMSApp}.
 */
public class CreateSchemaForRDBMSAppTest {

    private static File getTestResource(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(name).toURI());
    }

    @Test
    public void testImportViaFromParameters() throws Exception {
        MetadataStore metadataStore = new DefaultMetadataStore();

        File testFolder = getTestResource("test-database.db");

        CreateSchemaForRDBMSApp.fromParameters(
                metadataStore,
                "jdbc:sqlite:/" + testFolder.getAbsolutePath(),
                "test-schema",
                null,
                null,
                org.sqlite.JDBC.class.getCanonicalName(),
                "JDBC"
        );

        Schema schema = metadataStore.getSchemaByName("test-schema");
        Assert.assertNotNull("test-schema");
        Assert.assertEquals(9, schema.getTables().size());
        Table target = metadataStore.getTableByName("test-schema.Target");
        Assert.assertEquals(6, target.getColumns().size());
        Column targetData = metadataStore.getColumnByName("test-schema.Target.data");
        Assert.assertNotNull(targetData);

    }

    @Test
    public void testImportToSqliteViaFromParameters() throws Exception {
        File dbFile = File.createTempFile("test", "db");
        dbFile.deleteOnExit();
        SQLiteInterface sqliteInterface = SQLiteInterface.createForFile(dbFile);
        MetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(sqliteInterface);

        File testFolder = getTestResource("test-database.db");

        CreateSchemaForRDBMSApp.fromParameters(
                metadataStore,
                "jdbc:sqlite:/" + testFolder.getAbsolutePath(),
                "test-schema",
                null,
                null,
                org.sqlite.JDBC.class.getCanonicalName(),
                "JDBC"
        );

        Schema schema = metadataStore.getSchemaByName("test-schema");
        Assert.assertNotNull("test-schema");
        Assert.assertEquals(9, schema.getTables().size());
        Table target = metadataStore.getTableByName("test-schema.Target");
        Assert.assertEquals(6, target.getColumns().size());
        Column targetData = metadataStore.getColumnByName("test-schema.Target.data");
        Assert.assertNotNull(targetData);

    }

}