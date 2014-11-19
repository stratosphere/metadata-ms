package de.hpi.isg.metadata_store.domain.factories;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.exceptions.MetadataStoreNotFoundException;

public class MetadataStoreFactory {
    
    public static DefaultMetadataStore loadDefaultMetadataStore(final File file) throws MetadataStoreNotFoundException {

        FileInputStream fin;
        try {
            fin = new FileInputStream(file);
            final ObjectInputStream ois = new ObjectInputStream(fin);
            final DefaultMetadataStore metadataStore = (DefaultMetadataStore) ois.readObject();
            ois.close();
            return metadataStore;
        } catch (IOException | ClassNotFoundException e) {
            throw new MetadataStoreNotFoundException(e);
        }

    }

    /**
     * @deprecated use functions to <u>either</u> load or create a metadata store.
     */
    public static DefaultMetadataStore loadOrCreateAndSaveDefaultMetadataStore(final File file) throws IOException {
        try {
            return MetadataStoreFactory.loadDefaultMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            final DefaultMetadataStore metadataStore = new DefaultMetadataStore();
            if (!file.exists()) {
                file.createNewFile();
            }
            metadataStore.save(file.getAbsolutePath());
            return metadataStore;
        }
    }

//    public static RDBMSMetadataStore getMetadataStoreFromSQLite(Connection connection) {
//        SQLiteInterface sqliteInterface = new SQLiteInterface(connection);
//        RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqliteInterface);
//        return metadataStore;
//    }
//
//    public static RDBMSMetadataStore getOrCreateMetadataStoreInSQLite(File file) {
//        Connection connection = null;
//
//        try {
//            Class.forName("org.sqlite.JDBC");
//            String connString = String.format("jdbc:sqlite:%s", file.getAbsoluteFile());
//            connection = DriverManager.getConnection(connString);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return getOrCreateMetadataStoreInSQLite(connection);
//    }
//
//    public static RDBMSMetadataStore getOrCreateMetadataStoreInSQLite(Connection connection) {
//        SQLiteInterface sqliteInterface = new SQLiteInterface(connection);
//        if (!sqliteInterface.tablesExist()) {
//            sqliteInterface.initializeMetadataStore();
//        }
//        RDBMSMetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(sqlInterface, configuration)(sqliteInterface);
//        return metadataStore;
//    }
//
//    public static RDBMSMetadataStore createEmptyMetadataStoreInSQLite(Connection connection) {
//        SQLiteInterface sqliteInterface = new SQLiteInterface(connection);
//        sqliteInterface.initializeMetadataStore();
//        RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqliteInterface);
//        return metadataStore;
//    }
}
