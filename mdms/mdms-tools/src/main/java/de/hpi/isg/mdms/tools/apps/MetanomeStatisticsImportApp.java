package de.hpi.isg.mdms.tools.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.tools.metanome.StatisticsResultReceiver;
import de.metanome.algorithm_integration.results.BasicStatistic;
import de.metanome.algorithm_integration.results.JsonConverter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class imports single column statistics from JSON files.
 */
public class MetanomeStatisticsImportApp extends MdmsAppTemplate<MetanomeStatisticsImportApp.Parameters> {

    /**
     * Pattern to extract JSON keys for the top k frequent values.
     */
    private static final Pattern TOP_K_FREQUENT_VALUES_PATTERN = Pattern.compile("Frequency Of Top (\\d+) Frequent Items");

    /**
     * Pattern to generate JSON key for the top k frequent values.
     */
    private static final String TOP_K_FREQUENT_VALUES_FORMAT = "Top %d frequent items";

    private static final String FREQUENCY_TOP_K_ITEMS_FORMAT = "Frequency Of Top %d Frequent Items";

    /**
     * Key used in the {@link #executionMetadata} to present the ID of the generated constraint collection.
     */
    public static final String CONSTRAINT_COLLECTION_ID_KEY = "constraintCollectionId";

    public static void main(String[] args) throws Exception {
        MetanomeStatisticsImportApp.Parameters parameters = new MetanomeStatisticsImportApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new MetanomeStatisticsImportApp(parameters).run();
    }

    public MetanomeStatisticsImportApp(MetanomeStatisticsImportApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds, String fileLocation, String schemaName) throws Exception {
        fromParameters(mds, fileLocation, ".+", schemaName);
    }

    public static void fromParameters(MetadataStore mds, String fileLocation, String filePattern, String schemaName) throws Exception {

        MetanomeStatisticsImportApp.Parameters parameters = new MetanomeStatisticsImportApp.Parameters();

        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(fileLocation);
        parameters.inputDirectories = inputFiles;
        parameters.schemaName = schemaName;

        MetanomeStatisticsImportApp app = new MetanomeStatisticsImportApp(parameters);
        app.metadataStore = mds;

        app.run();
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Set up the facilities to write to the metadata store.
        Schema schema = this.metadataStore.getSchemaByName(this.parameters.schemaName);
        if (schema == null) {
            throw new IllegalArgumentException("No such schema: " + this.parameters.schemaName);
        }

        JsonConverter<BasicStatistic> jsonConverter = new JsonConverter<>();

        try (StatisticsResultReceiver resultReceiver = new StatisticsResultReceiver(
                this.metadataStore, schema, Collections.singletonList(schema), String.format("Statistics for %s", schema.getName())
        )) {

            for (String inputDirectoryPath : this.parameters.inputDirectories) {
                // Discover files with statistics.
                Collection<File> statisticsFiles = this.discoverStatisticsFiles(inputDirectoryPath);

                // Now import all the files.
                for (File statisticsFile : statisticsFiles) {
                    this.logger.info("Loading {}.", statisticsFile);

                    // Load the file.
                    List<String> lines;
                    try {
                        lines = Files
                                .lines(statisticsFile.toPath(), Charset.forName(this.parameters.encoding))
                                .collect(Collectors.toList());
                    } catch (Exception e) {
                        this.getLogger().error("Could not read " + statisticsFile + ".", e);
                        continue;
                    }

                    // All following lines contain statistics on different columns.
                    for (String columnStatisticsLine : lines) {
                        try {
                            BasicStatistic basicStatistic = jsonConverter.fromJsonString(columnStatisticsLine, BasicStatistic.class);
                            resultReceiver.receiveResult(basicStatistic);
                        } catch (Exception e) {
                            this.logger.error("Could not parse a line of {}.", statisticsFile, e);
                        }
                    }
                }
            }

        }

        // Finalize.
        this.metadataStore.close();
    }

    /**
     * Discovers the statistics files in the given directory.
     *
     * @param inputDirectoryPath a directory with statistics files
     * @return a mapping from statistics files to the names of the tables that they describe
     */
    private Collection<File> discoverStatisticsFiles(String inputDirectoryPath) {
        // Detect files to import.
        File inputDir = new File(inputDirectoryPath);
        if (!inputDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + inputDir);
        }
        Pattern statisticsFilePattern = Pattern.compile(this.parameters.filePattern);
        File[] statisticsFiles = inputDir.listFiles((file) -> file.isFile() && statisticsFilePattern.matcher(file.getName()).matches());
        if (statisticsFiles == null) statisticsFiles = new File[0];
        return Arrays.stream(statisticsFiles).collect(Collectors.toList());
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
     * Parameters for {@link MetanomeStatisticsImportApp}.
     */
    public static class Parameters {

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @Parameter(description = "directories with single column statistics files",
                required = true)
        public List<String> inputDirectories;

        @Parameter(names = "--file-pattern",
        description = "matches statistics files in the input directory")
        public String filePattern = ".+";

        @Parameter(names = "--description",
                description = "description for the imported constraint collection",
                required = false)
        public String description;

        @Parameter(names = "--encoding",
                description = "encoding of the statistics files",
                required = false)
        public String encoding = "ISO-8859-1";

        /**
         * @return the user-specified description or a default one
         */
        public String getDescription() {
            return this.description == null ? "Single column statistics import" : this.description;
        }

        @Parameter(names = MetadataStoreParameters.SCHEMA_NAME,
                description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION,
                required = true)
        public String schemaName;

    }

}