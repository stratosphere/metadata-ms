package de.hpi.isg.mdms.tools.apps;

import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

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
        Table table1 = metadataStore.getTableByName("test-schema.table1.csv");
        Assert.assertNotNull(table1);
        Assert.assertEquals(3, table1.getColumns().size());
        Column table1Comments = metadataStore.getColumnByName("test-schema.table2.csv.Comments");
        Assert.assertNotNull(table1Comments);
    }

}