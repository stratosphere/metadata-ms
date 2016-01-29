package de.hpi.isg.mdms.clients.apps;

import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;

/**
 * This class gives a template for jobs that profile with CSV files. In particular, it takes care of resolving input
 * directories to files and indexing the columns in all files with unique IDs.
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
     * @deprecated use {@link MetadataStore#getSchemaById(int)} and/or {@link MetadataStore#getSchemaByName(String)}
     */
    protected Schema getSchema(final Integer schemaId, final String schemaName) {
        if (schemaId == null && schemaName == null) {
            throw new IllegalArgumentException("Schema ID and name must not be null at the same time.");
        }

        Schema schemaByName = null;
        for (final Schema schema : this.metadataStore.getSchemas()) {
            if (schemaId != null) {
                if (schemaId.intValue() == schema.getId()) {
                    return schema;
                }
            } else {
                if (schemaName.equals(schema.getName())) {
                    if (schemaByName != null) {
                        throw new IllegalArgumentException(String.format("The schema name \"%s\" is ambiguous.",
                                schemaName));
                    }
                    schemaByName = schema;
                }
            }
        }
        return schemaByName;
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

        this.metadataStore = MetadataStoreUtil.loadMetadataStore(getMetadataStoreParameters());
    }

    @Override
    protected void onExit() {
        super.onExit();

        if (getMetadataStoreParameters().isForceQuit) {
            System.exit(0);
        }
    }
}