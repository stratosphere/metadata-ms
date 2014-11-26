package de.hpi.isg.metadata_store.domain.factories;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;

import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
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
            return createAndSaveDefaultMetadataStore(file);
        }
    }

    public static DefaultMetadataStore createAndSaveDefaultMetadataStore(final File file) throws IOException {
        return createAndSaveDefaultMetadataStore(file, IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    public static DefaultMetadataStore createAndSaveDefaultMetadataStore(final File file, int numTableBitsInIds,
            int numColumnBitsInIds) throws IOException {
        
        final DefaultMetadataStore metadataStore = new DefaultMetadataStore(file, numTableBitsInIds, numColumnBitsInIds);
        if (!file.exists()) {
            file.createNewFile();
        }
        try {
            metadataStore.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return metadataStore;
    }

    public static Connection createSQLiteConnection(File file) {
        try {
            Class.forName("org.sqlite.JDBC");
            String connString = String.format("jdbc:sqlite:%s", file.getAbsoluteFile());
            return DriverManager.getConnection(connString);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // public static RDBMSMetadataStore getMetadataStoreFromSQLite(Connection connection) {
    // SQLiteInterface sqliteInterface = new SQLiteInterface(connection);
    // RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqliteInterface);
    // return metadataStore;
    // }
    //
    // public static RDBMSMetadataStore getOrCreateMetadataStoreInSQLite(File file) {
    // Connection connection = null;
    //
    // try {
    // Class.forName("org.sqlite.JDBC");
    // String connString = String.format("jdbc:sqlite:%s", file.getAbsoluteFile());
    // connection = DriverManager.getConnection(connString);
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // return getOrCreateMetadataStoreInSQLite(connection);
    // }
    //
    // public static RDBMSMetadataStore getOrCreateMetadataStoreInSQLite(Connection connection) {
    // SQLiteInterface sqliteInterface = new SQLiteInterface(connection);
    // if (!sqliteInterface.tablesExist()) {
    // sqliteInterface.initializeMetadataStore();
    // }
    // RDBMSMetadataStore metadataStore = RDBMSMetadataStore.createNewInstance(sqlInterface,
    // configuration)(sqliteInterface);
    // return metadataStore;
    // }
    //
    // public static RDBMSMetadataStore createEmptyMetadataStoreInSQLite(Connection connection) {
    // SQLiteInterface sqliteInterface = new SQLiteInterface(connection);
    // sqliteInterface.initializeMetadataStore();
    // RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqliteInterface);
    // return metadataStore;
    // }
}
