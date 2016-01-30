
package de.hpi.isg.mdms.flink.apps;

import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.flink.data.kmeans.UCCCentroid;
import de.hpi.isg.mdms.flink.data.kmeans.UCCDataPoint;
import de.hpi.isg.mdms.flink.functions.kmeans.CentroidMapper;
import de.hpi.isg.mdms.flink.functions.kmeans.RetrieveColumnNames;
import de.hpi.isg.mdms.flink.functions.kmeans.UCCCentroidMapper;
import de.hpi.isg.mdms.flink.functions.kmeans.UCCMapper;
import de.hpi.isg.mdms.flink.readwrite.FlinkMetdataStoreAdapter;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.flink.serializer.UCCFlinkSerializer;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * This job clusters UCCS according to their Jaccard similarity using an iterative k-means approach.
 * The user can provide a fixed number of clusters (k) and/or a minimal similarity that each ucc must have to its cluster center.
 * The algorithm increases the number of clusters until the minimal similarity is achieved.
 * The output {@link #clusteredPoints} and {@link #finalCentroids} are mapped to their column names and printed to a text file.
 *
 * @author Susanne Buelow
 */
public class KmeansUccsApp extends KmeansAppTemplate<Tuple1<int[]>, UCCDataPoint, UCCCentroid> {

    public static void main(final String[] args) throws Exception {
        KmeansAppTemplate.Parameters parameters = new KmeansAppTemplate.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new KmeansUccsApp(parameters).run();
    }

    public KmeansUccsApp(final KmeansAppTemplate.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void executeAppLogic() throws Exception {

        calculateClustering();

        final ExecutorService executorService = Executors.newSingleThreadExecutor();


        //Retrieve column names
        Collection<Target> targets = this.constraintCollection.getScope();
        HashMap<Integer, String> columns = new HashMap<>();
        if (targets.size() == 1) {
            Target target = targets.iterator().next();
            if (target instanceof Schema) {
                Schema schema = (Schema) target;
                for (Table table : schema.getTables()) {
                    for (Column column : table.getColumns()) {
                        columns.put(column.getId(), column.getName());
                    }
                }
            }
        }

        RetrieveColumnNames nameRetriever = new RetrieveColumnNames();
        nameRetriever.setColumnNames(columns);
        DataSet<Tuple3<Long, UCCDataPoint, Double>> clusteredUccPoints = clusteredPoints.map(new UCCMapper<>());
        DataSet<Tuple3<Long, HashSet<String>, Double>> printablePoints = clusteredUccPoints.map(nameRetriever);

        CentroidMapper centroidMapper = new CentroidMapper();
        centroidMapper.setColumnNames(columns);
        DataSet<UCCCentroid> finalUccCentroids = finalCentroids.map(new UCCCentroidMapper<>());
        DataSet<Tuple2<Long, HashSet<String>>> printableCentroids = finalUccCentroids.map(centroidMapper);

        printablePoints.writeAsCsv("clusteredPoints");

        printableCentroids.writeAsCsv("centroids");

        this.executePlan("print output of final iteration with k = " + k);

        RemoteCollectorImpl.shutdownAll();

        this.logger.debug("Shutting down mdms executor.");
        executorService.shutdown();
        this.logger.debug("Awaiting termination of mdms executor.");
        executorService.awaitTermination(365, TimeUnit.DAYS);

        this.metadataStore.flush();
    }

    //OPERATORS


    protected DataSet<Tuple1<int[]>> retrieveInputData() {
        return FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.executionEnvironment, this.metadataStore, this.constraintCollection, new UCCFlinkSerializer());
    }


}