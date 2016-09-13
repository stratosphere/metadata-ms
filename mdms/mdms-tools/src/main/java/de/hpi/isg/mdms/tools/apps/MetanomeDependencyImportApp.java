package de.hpi.isg.mdms.tools.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.tools.metanome.ResultMetadataStoreWriter;
import de.hpi.isg.mdms.tools.metanome.reader.*;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Imports results in the user-readable Metanome format.
 */
public class MetanomeDependencyImportApp extends MdmsAppTemplate<MetanomeDependencyImportApp.Parameters> {

    public MetanomeDependencyImportApp(MetanomeDependencyImportApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds, String fileLocation, String schemaName,
                                      String type) throws Exception {

        MetanomeDependencyImportApp.Parameters parameters = new MetanomeDependencyImportApp.Parameters();

        parameters.resultFiles.add(fileLocation);
        parameters.dependencyType = type;
        parameters.schemaName = schemaName;

        MetanomeDependencyImportApp app = new MetanomeDependencyImportApp(parameters);
        app.metadataStore = mds;

        app.run();
    }

    public static void main(String[] args) throws Exception {
        MetanomeDependencyImportApp.Parameters parameters = new MetanomeDependencyImportApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new MetanomeDependencyImportApp(parameters).run();
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Set up the dependency reader and receiver.
        ResultReader resultReader = this.parameters.createResultReader();
        try (ResultReceiver resultReceiver = new ResultMetadataStoreWriter("import",
                this.metadataStore,
                this.parameters.schemaName,
                null,
                String.format("%s (%s)", this.parameters.getDescription(), new Date()))) {
            this.parameters.resultFiles.stream()
                    .map(File::new)
                    .filter(file -> {
                        getLogger().info("Loading {}.", file);
                        return true;
                    })
                    .forEach(resultFile -> resultReader.parse(resultFile, resultReceiver));
        }

        this.metadataStore.close();
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }


    /**
     * Parameters for {@link MetanomeDependencyImportApp}.
     */
    public static class Parameters {

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @Parameter(description = "files to import",
                required = true)
        public final List<String> resultFiles = new LinkedList<>();

        @Parameter(names = "--description",
                description = "description for the imported constraint collection",
                required = false)
        public String description;

        /**
         * @return the user-specified description or a default one
         */
        public String getDescription() {
            return this.description == null ? String.format("%s import", this.dependencyType) : this.description;
        }

        @Parameter(names = MetadataStoreParameters.SCHEMA_NAME,
                description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION,
                required = true)
        public String schemaName;

        @Parameter(names = "--dependency-type",
                description = "type of imported dependencies; one of IND, UCC, FD, OD",
                required = true)
        public String dependencyType;

        /**
         * @return a {@link ResultReader} according to the {@link #dependencyType}.
         */
        public ResultReader createResultReader() {
            switch (this.dependencyType) {
                case "IND":
                    return new InclusionDependencyReader();
                case "UCC":
                    return new UniqueColumnCombinationReader();
                case "FD":
                    return new FunctionalDependencyReader();
                case "OD":
                    return new OrderDependencyReader();
                default:
                    throw new IllegalArgumentException("Unknown dependency type: " + this.dependencyType);
            }
        }

    }
}
