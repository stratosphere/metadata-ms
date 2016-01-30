
package de.hpi.isg.mdms.flink.apps;

import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.flink.data.apriori.Item;
import de.hpi.isg.mdms.flink.data.apriori.ItemSet;
import de.hpi.isg.mdms.flink.functions.apriori.RuleMapper;
import de.hpi.isg.mdms.flink.functions.apriori.TakeFirstItemReduce;
import de.hpi.isg.mdms.flink.functions.apriori.UCCItemConverter;
import de.hpi.isg.mdms.flink.functions.apriori.UCCToTransactionsMapper;
import de.hpi.isg.mdms.flink.readwrite.FlinkMetdataStoreAdapter;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.flink.serializer.UCCFlinkSerializer;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This job finds association rules on columns of with UCCS being the transactions.
 * It uses the abstract job {@link AprioriAppTemplate}.
 * The output {@link #associationRulesLevels} and {@link #finalCentroids} are mapped to their column names and printed to a text file.
 *
 * @author Susanne
 */
public class AprioriUccsApp extends AprioriAppTemplate {

    protected DataSet<Tuple1<int[]>> uccs;

    public static void main(final String[] args) throws Exception {
        AprioriAppTemplate.Parameters parameters = new AprioriAppTemplate.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new AprioriUccsApp(parameters).run();
    }

    public AprioriUccsApp(final AprioriAppTemplate.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void executeAppLogic() throws Exception {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        this.constraintCollection = this.metadataStore.getConstraintCollection(this.parameters.constraintCollectionID);

        this.uccs = FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.executionEnvironment, this.metadataStore, this.constraintCollection, new UCCFlinkSerializer());

        calculateAssociationRules();

        //Retrieve column names
        Collection<Target> targets = this.constraintCollection.getScope();
        HashMap<Integer, String> columns = new HashMap<Integer, String>();
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

        RuleMapper ruleMapper = new RuleMapper();
        ruleMapper.setColumnNames(columns);

        for (int i : this.associationRulesLevels.keySet()) {
            DataSet<Tuple3<String, Double, Double>> printableRules = this.associationRulesLevels.get(i).map(ruleMapper);
            printableRules.writeAsText("associationRules " + i);
        }


        this.executePlan("print output");

        RemoteCollectorImpl.shutdownAll();

        this.logger.debug("Shutting down mdms executor.");
        executorService.shutdown();
        this.logger.debug("Awaiting termination of mdms executor.");
        executorService.awaitTermination(365, TimeUnit.DAYS);

        this.metadataStore.flush();
    }

    //OPERATORS


    @Override
    protected DataSet<ItemSet> retrieveTransactions() {
        return this.uccs.map(new UCCToTransactionsMapper());
    }

    @Override
    protected DataSet<Item> retrieveItems() {
        return this.uccs.flatMap(new UCCItemConverter())
                .groupBy(0)
                .reduceGroup(new TakeFirstItemReduce());
    }

}