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
package de.hpi.isg.mdms.clients.util;

import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.exceptions.MetadataStoreException;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * This class offers methods to manage access to {@link MetadataStore}s.
 *
 * @author Sebastian Kruse
 */
public class MetadataStoreUtil {

    private static final boolean DEFAULT_IS_OVERWRITE = false;

    private static final Logger LOG = LoggerFactory.getLogger(MetadataStoreUtil.class);

    private MetadataStoreUtil() {
    }

    /**
     * Creates a metadata store according to the given parameters.
     *
     * @param metadataStoreParameters describe the metadata store to be created
     * @return the created metadata store
     * @throws IOException
     */
    public static MetadataStore createMetadataStore(MetadataStoreParameters metadataStoreParameters) throws IOException {
        return createMetadataStore(metadataStoreParameters, DEFAULT_IS_OVERWRITE, IdUtils.DEFAULT_NUM_TABLE_BITS,
                IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    /**
     * Creates a metadata store according to the given parameters.
     *
     * @param params        describe the metadata store to be created
     * @param numTableBits  are the number of ID bits to be used to encode tables
     * @param numColumnBits are the number of ID bits to be used to encode columns
     * @throws IOException
     */
    public static MetadataStore createMetadataStore(MetadataStoreParameters params, boolean isOverwrite, int numTableBits,
                                                    int numColumnBits) throws IOException {

        File metadataStoreFile = new File(params.metadataStore);
        if (metadataStoreFile.exists()) {
            if (isOverwrite) {
                if (!metadataStoreFile.delete()) {
                    throw new RuntimeException(String.format("Could not delete the metadata store file: %s", metadataStoreFile));
                }
            } else {
                throw new RuntimeException(String.format("Cannot create a metadata store at %s, file already exists.",
                        metadataStoreFile.getAbsoluteFile()));
            }
        }

        if (!params.isDemandJavaSerialized) {
            SQLInterface sqlInterface = SQLiteInterface.createForFile(metadataStoreFile);
            try {
                return RDBMSMetadataStore.createNewInstance(sqlInterface, numTableBits, numColumnBits);
            } catch (SQLException e) {
                throw new MetadataStoreException("Could not create metadata store.", e);
            }
        } else {
            return DefaultMetadataStore.createAndSave(metadataStoreFile, numTableBits, numColumnBits);
        }
    }

    /**
     * Saves this store. The metadata store may be autosaving its changes, but calling this method will cause no harm.
     *
     * @throws IOException if the saving failed due to file errors.
     * @deprecated Use {@link MetadataStore#flush()}.
     */
    public static void save(MetadataStore metadataStore, MetadataStoreParameters params) throws IOException {
        metadataStore.save(params.metadataStore);
    }

    /**
     * Loads a metadata store.
     *
     * @param metadataStoreParameters specify the metadata store to be loaded
     * @return the loaded metadata store
     */
    public static MetadataStore loadMetadataStore(MetadataStoreParameters metadataStoreParameters) {
        File metadataStoreFile = new File(metadataStoreParameters.metadataStore);

        if (metadataStoreParameters.isDemandJavaSerialized) {
            return DefaultMetadataStore.load(metadataStoreFile);
        } else {
            SQLiteInterface sqlInterface = SQLiteInterface.createForFile(metadataStoreFile);
            RDBMSMetadataStore metadataStore;
            try {
                metadataStore = RDBMSMetadataStore.load(sqlInterface);
            } catch (SQLException e) {
                throw new MetadataStoreException("Could not load the metadata store.", e);
            }
            metadataStore.setUseJournal(!metadataStoreParameters.isNotUseJournal);
            return metadataStore;
        }
    }
}
