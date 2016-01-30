
package de.hpi.isg.mdms.flink.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.flink.data.apriori.AssociationRule;
import de.hpi.isg.mdms.flink.data.apriori.CalculateSupport;
import de.hpi.isg.mdms.flink.data.apriori.Item;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import de.hpi.isg.mdms.flink.functions.apriori.*;
import de.hpi.isg.mdms.flink.parameters.FlinkParameters;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.AbstractID;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This job is an abstract class that can be used to find association rules with apriori.
 * The user provides a min-support and a min-confidence.
 * The algorithm uses the apriori-algorithm.
 * The output {@link #associationRulesLevels} can be further processed afterwards.
 * <p>
 * An implementation needs to provide:
 * - retrieveItems() that returns a Dataset of the Items
 * - retrieveTransactions() that returns a Dataset of the Transactions
 * <p>
 * See {@link AprioriUccsApp} for an example.
 *
 * @author Susanne
 */
public abstract class AprioriAppTemplate extends FlinkAppTemplate<AprioriAppTemplate.Parameters> {

    public DataSet<Item> items;

    public DataSet<ItemSet> transactions;

    public HashMap<Integer, DataSet<Tuple3<AssociationRule, Double, Double>>> associationRulesLevels;

    protected ConstraintCollection constraintCollection;

    public AprioriAppTemplate(final AprioriAppTemplate.Parameters parameters) {
        super(parameters);
    }

    protected void calculateAssociationRules() throws Exception {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        this.items = retrieveItems();

        this.transactions = retrieveTransactions();

        // CANDIDATE GENERATION
        HashMap<Integer, DataSet<Tuple2<ItemSet, Double>>> candidates = new HashMap<>();

        //generate candidates k=1
        DataSet<Tuple2<ItemSet, Double>> candidates_1 = this.items.map(new ItemToItemSet())
                .map(new CalculateSupport()).withBroadcastSet(this.transactions, "transactions")
                .filter(new FilterMinSupport(this.parameters.minSupport));
        candidates.put(1, candidates_1);

        int k = 1;

        //generate candidates k+1 with apriori until no new candidates are found
        boolean newCandidatesCreated = true;

        while (newCandidatesCreated) {
            k = k + 1;

            DataSet<Tuple2<ItemSet, Double>> newCandidates = candidates.get(k - 1).flatMap(new AprioriGen(k - 2)).withBroadcastSet(candidates.get(k - 1), "candidates_k-1")
                    .map(new CalculateSupport()).withBroadcastSet(this.transactions, "transactions")
                    .filter(new FilterMinSupport(this.parameters.minSupport))
                    .groupBy(1)
                    .reduceGroup(new CompareItemSetReduce());

            final String countId = new AbstractID().toString();
            newCandidates.flatMap(new Utils.CountHelper<>(countId))
                    .output(new DiscardingOutputFormat<>());

            this.logger.info("Generate candidates on level " + k);
            JobExecutionResult res = this.executionEnvironment.execute();

            long count = res.<Long>getAccumulatorResult(countId);

            if (count == 0) {
                newCandidatesCreated = false;
            } else {
                candidates.put(k, newCandidates);
            }
        }

        //GENERATE ASSOCIATION RULES
        this.associationRulesLevels = new HashMap<>();

        //generate first level association rules
        DataSet<Tuple3<AssociationRule, Double, Double>> associationRules = candidates.get(2).flatMap(new MapCandidateToAssociationRule())
                .map(new CalculateConfidence()).withBroadcastSet(candidates.get(1), "candidates")
                .filter(new FilterMinConfidence(this.parameters.minConfidence));

        this.associationRulesLevels.put(1, associationRules);

        //iterate until no new association rules
        boolean newRulesCreated = true;

        k = 1;
        while (newRulesCreated) {
            k = k + 1;

            DataSet<Tuple3<AssociationRule, Double, Double>> newRules = this.associationRulesLevels.get(k - 1)
                    .flatMap(new AprioriGenRules(k - 1))
                    .withBroadcastSet(this.associationRulesLevels.get(k - 1), "rules_k-1")
                    .map(new GetSupport()).withBroadcastSet(candidates.get(k + 1), "candidates")
                    .map(new CalculateConfidence()).withBroadcastSet(candidates.get(k), "candidates")
                    .filter(new FilterMinConfidence(this.parameters.minConfidence))
                    .groupBy(1, 2)
                    .reduceGroup(new CompareAssociationRulesReduce());

            final String countId = new AbstractID().toString();
            newRules.flatMap(new Utils.CountHelper<>(countId))
                    .output(new DiscardingOutputFormat<>());

            this.logger.info("Generate association rules on level " + k);
            JobExecutionResult res = this.executionEnvironment.execute();
            long count = res.<Long>getAccumulatorResult(countId);

            if (count == 0 || candidates.get(k + 2) == null) {
                newRulesCreated = false;
            } else {
                this.associationRulesLevels.put(k, newRules);
            }
        }

        RemoteCollectorImpl.shutdownAll();

        this.logger.debug("Shutting down mdms executor.");
        executorService.shutdown();
        this.logger.debug("Awaiting termination of mdms executor.");
        executorService.awaitTermination(365, TimeUnit.DAYS);

        this.metadataStore.flush();
    }

    protected abstract DataSet<ItemSet> retrieveTransactions();

    protected abstract DataSet<Item> retrieveItems();


    //OPERATORS


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

        @Parameter(names = {"--min-support"}, required = false)
        public double minSupport;

        @Parameter(names = {"--min-confidence"}, required = false)
        public double minConfidence;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @ParametersDelegate
        public final FlinkParameters flinkParameters = new FlinkParameters();

    }

}