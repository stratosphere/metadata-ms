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
import de.hpi.isg.mdms.clients.apps.AppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.util.IdUtils;

import java.io.Serializable;

/**
 * This job creates a metadata store at the specified location.
 *
 * @author Sebastian Kruse
 */
public class CreateMetadataStoreApp extends AppTemplate<CreateMetadataStoreApp.Parameters> {

    /**
     * Creates a new instance of this class.
     *
     * @see AppTemplate#AppTemplate(Object)
     */
    public CreateMetadataStoreApp(CreateMetadataStoreApp.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void executeAppLogic() throws Exception {

        // Create the store.
        MetadataStore store = MetadataStoreUtil.createMetadataStore(
                this.parameters.metadataStoreParameters,
                this.parameters.isOverwrite,
                this.parameters.numTableBitsInIds,
                this.parameters.numColumnBitsInIds);

        // Make sure that any changes are committed.
        store.flush();

        this.logger.info("Created {} at {}..\n", store, this.parameters.metadataStoreParameters.metadataStore);
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    public static void main(final String[] args) throws Exception {
        CreateMetadataStoreApp.Parameters parameters = new CreateMetadataStoreApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new CreateMetadataStoreApp(parameters).run();
    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    @SuppressWarnings("serial")
    public static class Parameters implements Serializable {

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @Parameter(names = "--overwrite")
        public boolean isOverwrite = false;

        @Parameter(names = MetadataStoreParameters.NUM_TABLE_BITS_IN_IDS,
                description = "number of bits (of 32 overall bits) to use for encoding table IDs")
        public int numTableBitsInIds = IdUtils.DEFAULT_NUM_TABLE_BITS;

        @Parameter(names = MetadataStoreParameters.NUM_COLUMN_BITS_IN_IDS,
                description = "number of bits (of 32 overall bits) to use for encoding table IDs")
        public int numColumnBitsInIds = IdUtils.DEFAULT_NUM_COLUMN_BITS;
    }

}
