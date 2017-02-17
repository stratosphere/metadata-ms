package de.hpi.isg.mdms.clients.apps;

import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;

/**
 * This class gives a template for apps that operate on the MDMS.
 *
 * @author Sebastian Kruse
 */
public abstract class MdmsAppTemplate<TParameters> extends AppTemplate<TParameters> {

    /**
     * The metadata store that contains schemas.
     */
    protected MetadataStore metadataStore;

    /**
     * Creates a new instance.
     *
     * @see AppTemplate#AppTemplate(Object)
     */
    public MdmsAppTemplate(TParameters tParameters) {
        super(tParameters);
    }

    /**
     * Subclasses must provide {@link MetadataStoreParameters} via this method, typically drawn from {@link #parameters}.
     *
     * @return the configured {@link MetadataStoreParameters}
     */
    abstract protected MetadataStoreParameters getMetadataStoreParameters();


    /**
     * Retrieves a Schema from the {@link MetadataStore}.
     *
     * @param schemaId   is the ID of the schema or {@code null} if the schema name shall be used as criterion
     * @param schemaName is the name of the schema to be retrieved
     * @return the schema
     * @throws IllegalArgumentException if both the schema name and ID are {@code null} or the given schema name is ambiguous
     */
    protected Schema getSchema(final Integer schemaId, final String schemaName) {
        if (schemaId == null && schemaName == null) {
            throw new IllegalArgumentException("Schema ID and name must not be null at the same time.");
        }
        Schema schema = null;
        if (schemaId != null) {
            schema = this.metadataStore.getSchemaById(schemaId);
        }
        if (schema == null && schemaName != null) {
            schema = this.metadataStore.getSchemaByName(schemaName);
        }
        return schema;
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

        if (this.metadataStore == null) {
            this.metadataStore = this.loadMetadataStore();
        }
    }

    protected MetadataStore loadMetadataStore() {
        return MetadataStoreUtil.loadMetadataStore(this.getMetadataStoreParameters());
    }

    @Override
    protected void onExit() {
        super.onExit();

        if (this.getMetadataStoreParameters().isForceQuit) {
            this.logger.info("Forcing application to quit...");
            System.exit(0);
        }
    }
}