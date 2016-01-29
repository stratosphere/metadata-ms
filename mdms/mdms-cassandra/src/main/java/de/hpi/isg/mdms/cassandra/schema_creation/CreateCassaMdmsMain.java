package de.hpi.isg.mdms.cassandra.schema_creation;

import de.hpi.isg.mdms.domain.CassaMetadataStore;

public class CreateCassaMdmsMain {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.format("Usage: java ... %s <Cassandra IP>\n", CreateCassaMdmsMain.class.getCanonicalName());
            System.exit(1);
        }

        CassaMetadataStore metadatastore = CassaMetadataStore.createNewInstance(args[0]);
        metadatastore.close();
    }

}
