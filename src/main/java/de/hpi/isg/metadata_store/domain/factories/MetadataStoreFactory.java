package de.hpi.isg.metadata_store.domain.factories;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.exceptions.MetadataStoreNotFoundException;

public class MetadataStoreFactory {
    public static MetadataStore getMetadataStore(final File file) throws MetadataStoreNotFoundException {

        FileInputStream fin;
        try {
            fin = new FileInputStream(file);
            final ObjectInputStream ois = new ObjectInputStream(fin);
            final MetadataStore metadataStore = (MetadataStore) ois.readObject();
            ois.close();
            return metadataStore;
        } catch (IOException | ClassNotFoundException e) {
            throw new MetadataStoreNotFoundException(e);
        }

    }

    public static MetadataStore getOrCreateAndSaveMetadataStore(final File file) throws IOException {
        try {
            return MetadataStoreFactory.getMetadataStore(file);
        } catch (final MetadataStoreNotFoundException e) {
            final MetadataStore metadataStore = new DefaultMetadataStore();
            if (!file.exists()) {
                file.createNewFile();
            }
            MetadataStoreFactory.saveMetadataStore(file, metadataStore);
            return metadataStore;
        }
    }

    public static void saveMetadataStore(final File file, final MetadataStore metadataStore) throws IOException {
        final FileOutputStream fout = new FileOutputStream(file);
        final ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(metadataStore);
        oos.close();
    }
}
