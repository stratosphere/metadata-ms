package de.hpi.isg.mdms.tools.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.hpi.isg.mdms.metanome.ResultReader;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Imports results in the user-readable Metanome format.
 */
public class MetanomeDependencyImportApp extends MdmsAppTemplate<MetanomeDependencyImportApp.Parameters> {

    public MetanomeDependencyImportApp(MetanomeDependencyImportApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds,
                                      String fileLocation,
                                      String filePattern,
                                      String fileType,
                                      String dependencyType,
                                      String scope,
                                      String schemaName,
                                      String userDefinedIdPrefix) throws Exception {

        MetanomeDependencyImportApp.Parameters parameters = new MetanomeDependencyImportApp.Parameters();

        parameters.schema = schemaName;
        parameters.resultFiles.add(fileLocation);
        parameters.filePattern = filePattern;
        parameters.fileType = fileType;
        parameters.dependencyType = dependencyType;
        parameters.scope = Collections.singletonList(scope);
        parameters.userDefinedIdPrefix = userDefinedIdPrefix;
        parameters.metadataStoreParameters.isCloseMetadataStore = false;

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
        // Identify the schema.
        Schema schema = this.metadataStore.getSchemaByName(this.parameters.schema);
        if (schema == null) {
            throw new IllegalArgumentException("Schema not found.");
        }

        // Identify the scope.
        Collection<Target> scope = new ArrayList<>();
        for (String scopeName : this.parameters.scope) {
            Target target = this.metadataStore.getTargetByName(scopeName);
            if (target == null) {
                throw new IllegalArgumentException(String.format(
                        "Could not find part of scope: \"%s\".", scopeName
                ));
            }
            scope.add(target);
        }

        // Set up the dependency reader and receiver.
        ResultReader resultReader = this.createResultReader(this.parameters);
        Class<?> constraintClass = this.parameters.getConstraintClass();
        try (DependencyResultReceiver<?> resultReceiver = new DependencyResultReceiver<>(
                this.metadataStore,
                schema,
                scope,
                constraintClass,
                String.format("%s (%s)", this.parameters.getDescription(), new Date()),
                this.parameters.userDefinedIdPrefix == null ?
                        null :
                        this.parameters.userDefinedIdPrefix + this.parameters.dependencyType.toLowerCase() + "s"
        )) {


            for (String resultFile : this.parameters.resultFiles) {
                Collection<File> fileCollection = this.discoverDependencyFiles(resultFile);
                fileCollection.forEach(file -> {
                    try {
                        resultReader.readAndLoad(file, resultReceiver);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
    }

    /**
     * @return a {@link ResultReader} according to the {@link MetanomeDependencyImportApp.Parameters#dependencyType}.
     */
    private ResultReader createResultReader(MetanomeDependencyImportApp.Parameters parameters) {
        if ("friendly".equalsIgnoreCase(parameters.fileType)) {
            switch (parameters.dependencyType) {
                case "IND":
                case "ind":
                    return new de.hpi.isg.mdms.metanome.friendly.InclusionDependencyReader();
                case "UCC":
                case "ucc":
                    return new de.hpi.isg.mdms.metanome.friendly.UniqueColumnCombinationReader();
                case "FD":
                case "fd":
                    return new de.hpi.isg.mdms.metanome.friendly.FunctionalDependencyReader();
                case "OD":
                case "od":
                    return new de.hpi.isg.mdms.metanome.friendly.OrderDependencyReader();
                default:
                    throw new IllegalArgumentException("Unknown dependency type: " + parameters.dependencyType);
            }

        } else if ("json".equalsIgnoreCase(parameters.fileType)) {
            switch (parameters.dependencyType) {
                case "IND":
                case "ind":
                    return new de.hpi.isg.mdms.metanome.json.InclusionDependencyReader();
                case "UCC":
                case "ucc":
                    return new de.hpi.isg.mdms.metanome.json.UniqueColumnCombinationReader();
                case "FD":
                case "fd":
                    return new de.hpi.isg.mdms.metanome.json.FunctionalDependencyReader();
                case "OD":
                case "od":
                    return new de.hpi.isg.mdms.metanome.json.OrderDependencyReader();
                default:
                    throw new IllegalArgumentException("Unknown dependency type: " + parameters.dependencyType);
            }
        } else if ("compact".equalsIgnoreCase(parameters.fileType)) {
            switch (parameters.dependencyType) {
                case "IND":
                case "ind":
                    return new de.hpi.isg.mdms.metanome.compact.InclusionDependencyReader();
                case "UCC":
                case "ucc":
                    return new de.hpi.isg.mdms.metanome.compact.UniqueColumnCombinationReader();
                case "FD":
                case "fd":
                    return new de.hpi.isg.mdms.metanome.compact.FunctionalDependencyReader();
                case "OD":
                case "od":
                    return new de.hpi.isg.mdms.metanome.compact.OrderDependencyReader();
                default:
                    throw new IllegalArgumentException("Unknown dependency type: " + parameters.dependencyType);
            }
        } else {
            throw new IllegalArgumentException(String.format("File type \"%s\" is currently not supported.", parameters.fileType));
        }
    }

    /**
     * Discovers the dependency files in the given directory.
     *
     * @param inputDirectoryPath a directory with statistics files
     * @return a mapping from statistics files to the names of the tables that they describe
     */
    private Collection<File> discoverDependencyFiles(String inputDirectoryPath) {
        // Detect files to import.
        File file = new File(inputDirectoryPath);
        if (!file.isDirectory()) {
            return Collections.singleton(file);
        }
        File[] dependencyFiles = file.listFiles((dir, name) -> name.matches(this.parameters.filePattern));
        if (dependencyFiles == null) dependencyFiles = new File[0];
        return Arrays.stream(dependencyFiles).collect(Collectors.toList());
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
                description = "description for the imported constraint collection")
        public String description;

        @Parameter(names = "--schema",
                description = "name of the schema to which the profiling results pertain",
                required = true)
        public String schema;

        @Parameter(names = "--scope",
                description = "scope of the imported constraint collection",
                variableArity = true)
        public List<String> scope;

        @Parameter(names = "--file-type",
                description = "type of the files to import (friendly, JSON, compact)")
        public String fileType = "friendly";

        @Parameter(names = "--file-pattern",
                description = "matches statistics files in the input directory")
        public String filePattern = ".+";

        @Parameter(names = "--id-prefix",
                description = "prefix for user-defined IDs of the constraint collections")
        public String userDefinedIdPrefix;

        /**
         * @return the user-specified description or a default one
         */
        public String getDescription() {
            return this.description == null ? String.format("%s import", this.dependencyType) : this.description;
        }

        @Parameter(names = "--dependency-type",
                description = "type of imported dependencies; one of IND, UCC, FD, OD",
                required = true)
        public String dependencyType;

        private Class<?> getConstraintClass() {
            switch (this.dependencyType) {
                case "IND":
                case "ind":
                    return InclusionDependency.class;
                case "UCC":
                case "ucc":
                    return UniqueColumnCombination.class;
                case "FD":
                case "fd":
                    return FunctionalDependency.class;
                default:
                    throw new IllegalArgumentException("Unknown dependency type: " + this.dependencyType);
            }
        }
    }
}
