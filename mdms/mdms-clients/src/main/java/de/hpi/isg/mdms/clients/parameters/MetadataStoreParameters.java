package de.hpi.isg.mdms.clients.parameters;

import com.beust.jcommander.Parameter;

public class MetadataStoreParameters {

    public final static String SCHEMA_NAME = "--schema-name";
    public final static String SCHEMA_NAME_DESCRIPTION = "the name of the schema";
    public static final String SCHEMA_ID = "--schema-id";
    public static final String SCHEMA_ID_DESCRIPTION = "--schema-id";
    public static final String NUM_TABLE_BITS_IN_IDS = "--table-bits";
    public static final String NUM_COLUMN_BITS_IN_IDS = "--column-bits";
    
    @Parameter(names = { "--metadata-store" }, description = "a location for the metadata store", required = true)
    public String metadataStore = null;

    @Parameter(names = { "--force-quit" }, description = "force quit after the program has completed")
    public boolean isForceQuit;

    @Parameter(names = { "--java-serialized"}, description = "request a metadata store based on default Java serialization")
    public boolean isDemandJavaSerialized = false;

    @Parameter(names = { "--no-journal"}, description = "for RDBMSMetadataStores avoid using journaling")
    public boolean isNotUseJournal = false;
    
}
