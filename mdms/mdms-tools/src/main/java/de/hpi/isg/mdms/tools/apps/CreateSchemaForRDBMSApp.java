/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.tools.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.location.JdbcLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.tools.util.PGPassFiles;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Properties;

/**
 * This job creates a {@link Schema} for the given RDBMS and saves it to a {@link MetadataStore}.
 *
 * @author Sebastian Kruse
 */
public class CreateSchemaForRDBMSApp extends MdmsAppTemplate<CreateSchemaForRDBMSApp.Parameters> {

    private static final String SCHEMA_ID_RESULT_KEY = "schemaId";

    /**
     * Creates a new instance.
     *
     * @see CsvAppTemplate#CsvAppTemplate(Object)
     */
    public CreateSchemaForRDBMSApp(final CreateSchemaForRDBMSApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds,
                                      String jdbcUrl,
                                      String schemaName,
                                      String dbSchemaName,
                                      String pgpassPath,
                                      String driverClassName) throws Exception {

        CreateSchemaForRDBMSApp.Parameters parameters = new CreateSchemaForRDBMSApp.Parameters();
        parameters.jdbcUrls = jdbcUrl;
        parameters.schemaName = schemaName;
        parameters.dbSchema = dbSchemaName;
        parameters.pgPassPath = pgpassPath;
        parameters.driverClassName = driverClassName;

        CreateSchemaForRDBMSApp app = new CreateSchemaForRDBMSApp(parameters);
        app.metadataStore = mds;

        app.run();
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Load the JDBC driver class if any.
        if (parameters.driverClassName != null) {
            Class.forName(this.parameters.driverClassName);
        }

        // Load the PGPass file if any.
        Properties connectionProperties = new Properties();
        if (this.parameters.pgPassPath != null) {
            PGPassFiles.PGPass pgPass = PGPassFiles.load(this.parameters.pgPassPath);
            if (!pgPass.username.isEmpty()) connectionProperties.put("user", pgPass.username);
            if (!pgPass.password.isEmpty()) connectionProperties.put("password", pgPass.password);
        }

        Schema schema = null;
        try {
            // Initialize the schema.
            String description = String.format("Schema %s at %s (%s)",
                    this.parameters.dbSchema == null ? "(none)" : this.parameters.dbSchema,
                    this.parameters.jdbcUrls,
                    new Date()
            );
            JdbcLocation schemaLocation = new JdbcLocation();
            schemaLocation.setUrl(this.parameters.jdbcUrls);
            schemaLocation.setDriverClass(this.parameters.driverClassName);
            schemaLocation.setSchema(this.parameters.dbSchema);
            schema = this.metadataStore.addSchema(this.parameters.schemaName, description, schemaLocation);

            // Open the DB connection.
            try (Connection connection = DriverManager.getConnection(this.parameters.jdbcUrls, connectionProperties)) {
                // Retrieve the tables.
                ResultSet tableRS = connection.getMetaData().getTables(
                        connection.getCatalog(), // Do we have a better guess than that?
                        this.parameters.dbSchema, // If a schema is given, narrow down the search.
                        "%", // Retrieve all tables.
                        new String[]{"TABLE"} // Only retrieve actual tables.
                );
                if (!tableRS.next()) {
                    System.out.printf("No tables found!");
                    return;
                }
                do {
                    String tableName = tableRS.getString(3); // see DatabaseMetaData.getTables(...)
                    Table table = schema.addTable(this.metadataStore, tableName, null, new DefaultLocation());
                    this.logger.info("Added {}.", table);
                } while (tableRS.next());

                // Retrieve the columns for the tables.
                for (Table table : schema.getTables()) {
                    ResultSet columnRS = connection.getMetaData().getColumns(
                            connection.getCatalog(),
                            this.parameters.dbSchema,
                            table.getName(),
                            "%"
                    );
                    int columnIndex = 0;
                    while (columnRS.next()) {
                        String columnName = columnRS.getString(4); // see DatabaseMetaData.getColumns(...)
                        // NB: We do not use the JDBC column ordinal, because they do not always start at 1 (e.g., SQLite).
                        // However, the columns should be ordred by the ordinal, so we can do our own indexing.
                        // int columnIndex = columnRS.getInt(17) - 1; // see DatabaseMetaData.getColumns(...)
                        Column column = table.addColumn(this.metadataStore, columnName, null, columnIndex++);
                        this.logger.info("Added {}.", column);
                    }
                    columnRS.close();
                }
            }

            // Add some metadata about the program results.
            this.executionMetadata.addCustomData(SCHEMA_ID_RESULT_KEY, schema.getId());
        } catch (Exception e) {
            // Roll back to not mess up the metadata store in the presence of errors.
            if (schema != null) {
                this.logger.error("Roll back changes...");
                this.metadataStore.removeSchema(schema);
            }
            throw e;

        } finally {
            this.metadataStore.flush();
        }
        logger.info(String.format("Saved schema (%d, %s):\n%s.\n", schema.getId(), schema.getName(), schema));
    }

    @Override
    protected boolean isCleanUpRequested() {
        return true;
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }


    public static void main(final String[] args) throws Exception {
        CreateSchemaForRDBMSApp.Parameters parameters = new CreateSchemaForRDBMSApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new CreateSchemaForRDBMSApp(parameters).run();
    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    @SuppressWarnings("serial")
    public static class Parameters implements Serializable {

        @Parameter(names = "--url", required = true, description = "JDBC URL of the database")
        public String jdbcUrls;

        @Parameter(names = "--db-schema", description = "name of the DB schema to be retrieved")
        public String dbSchema;

        @Parameter(names = {MetadataStoreParameters.SCHEMA_NAME}, description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION, required = true)
        public String schemaName;

        @Parameter(names = {"--pgpass"}, description = "a PGPass file with additional connection information (login, password)")
        public String pgPassPath;

        @Parameter(names = {"--driver"}, description = "JDBC driver class to be loaded")
        public String driverClassName;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

    }


}
