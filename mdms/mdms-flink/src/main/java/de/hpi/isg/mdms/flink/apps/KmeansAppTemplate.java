
package de.hpi.isg.mdms.flink.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.flink.data.Centroid;
import de.hpi.isg.mdms.flink.data.DataPoint;
import de.hpi.isg.mdms.flink.functions.kmeans.*;
import de.hpi.isg.mdms.flink.parameters.FlinkParameters;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.AbstractID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This job is an abstract class that can be used to cluster datapoints using an iterative k-means appraoch.
 * The user can provide a fixed number of clusters (k) and/or a minimal similarity that each datapoint must have to its cluster center.
 * The algorithm increases the number of clusters until the minimal similarity is achieved.
 * The output {@link #clusteredPoints} and {@link #finalCentroids} can be further processed afterwards.
 * <p>
 * An implementation needs to provide:
 * - retrieveInputData() that returns a Dataset of Tuples as an input for the clustering
 * - a DataPoint class that provide functions add and div to calculate cluster centroids
 * - a Centroid class that provides a getSimilarity function
 * <p>
 * See {@link ClusterUCCsApp} for an example.
 *
 * @author Susanne
 */
public abstract class KmeansAppTemplate<T extends Tuple, D extends DataPoint<T>, C extends Centroid<T, D>>
        extends FlinkAppTemplate<KmeansAppTemplate.Parameters> {

    protected ConstraintCollection constraintCollection;

    protected DataSet<Tuple3<Long, D, Double>> clusteredPoints = null;

    protected DataSet<C> finalCentroids = null;

    protected int k = 2;

    public boolean foundInvalid = true;

    public KmeansAppTemplate<T, D, C> kmeans = this;

    public KmeansAppTemplate(final KmeansAppTemplate.Parameters parameters) {
        super(parameters);
    }

    public void setInvalid(boolean invalid) {
        this.foundInvalid = invalid;
    }

    protected void calculateClustering() throws Exception {

        this.logger.info("Loading ConstraintCollection.");

        constraintCollection = this.metadataStore.getConstraintCollection(this.parameters.constraintCollectionID);

        this.logger.info("Planning cell data set.");
        DataSet<T> inputData = retrieveInputData();

        DataSet<D> datapoints = (DataSet<D>) DataSetUtils.zipWithIndex(inputData)
                .map(new TuplePointConverter<>());

        if (this.parameters.k > 0) k = this.parameters.k;

        //select first centroids
        List<Long> keepIDs = new ArrayList<>();
        for (long i = 0; i < k; i++) {
            keepIDs.add(i);
        }

        // TODO: Use Flink's DataSet#iterate(...) instead of a while loop.
        while (this.foundInvalid) {

            PointCentroidConverter<T, D, C> centroidConverter = new PointCentroidConverter<>();
            centroidConverter.setKeepIDs(keepIDs);
            DataSet<C> centroids = (DataSet<C>) datapoints.flatMap(centroidConverter);

            //Loop over finding cluster centers
            IterativeDataSet<C> loop = centroids.iterate(this.parameters.numIterations);

            DataSet<C> newCentroids = (DataSet<C>) datapoints
                    // compute closest centroid for each point
                    .map(new SelectNearestCenter<D, C>()).withBroadcastSet(loop, "centroids")
                    // count and sum point coordinates for each centroid
                    .map(new CountAppender<D>())
                    .groupBy(0).reduce(new CentroidAccumulator<D>())
                    // compute new centroids from point counts and coordinate sums
                    .map(new CentroidAverager<T, D, C>());

            // feed new centroids back into next iteration
            finalCentroids = loop.closeWith(newCentroids);

            clusteredPoints = datapoints
                    // assign points to final clusters
                    .map(new SelectNearestCenterReturnSimilarity<T, D, C>()).withBroadcastSet(finalCentroids, "centroids");

            //check whether min similarity reached
            FilterMinSimilarity<T, D> filterMinSim = new FilterMinSimilarity<>(this.parameters.similarity);
            DataSet<Tuple3<Long, D, Double>> invalids = clusteredPoints.filter(filterMinSim);

            final String countId = new AbstractID().toString();
            invalids.flatMap(new Utils.CountHelper<>(countId))
                    .output(new DiscardingOutputFormat<>());

            this.logger.info("Cluster UCCs with KMeans (k=" + k + ")");
            JobExecutionResult res = executionEnvironment.execute();

            long count = res.<Long>getAccumulatorResult(countId);

            if (count == 0) {
                this.foundInvalid = false;
            } else {
                k = k + 1;
                keepIDs.add((long) keepIDs.size());
            }

        }

    }

    //OPERATORS    

    protected abstract DataSet<T> retrieveInputData();

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    @Override
    protected FlinkParameters getFlinkParameters() {
        return this.parameters.flinkParameters;
    }


    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    public static class Parameters implements Serializable {

        private static final long serialVersionUID = 2936720486536771056L;

        @Parameter(names = {"--constraint-collection-id"}, required = true)
        public int constraintCollectionID;

        @Parameter(names = {"--min-similarity"}, required = false)
        public double similarity;

        @Parameter(names = {"--k"}, required = false)
        public int k;

        @Parameter(names = {"--num-iterations"}, required = true)
        public int numIterations;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @ParametersDelegate
        public final FlinkParameters flinkParameters = new FlinkParameters();

    }


}