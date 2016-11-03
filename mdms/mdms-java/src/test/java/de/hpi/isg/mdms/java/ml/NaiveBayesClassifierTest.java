package de.hpi.isg.mdms.java.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.java.fk.feature.*;
import de.hpi.isg.mdms.java.fk.ml.classifier.AbstractClassifier;
import de.hpi.isg.mdms.java.fk.ml.classifier.NaiveBayes;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NaiveBayesClassifierTest {

    private static Dataset trainingSet;

    private static Dataset testSet;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        final MetadataStore store = new DefaultMetadataStore();
        final Schema schema = store.addSchema("foo", null, mock(Location.class));

        final Table bar1 = schema.addTable(store, "bar1", null, mock(Location.class));
        final Column column10 = bar1.addColumn(store, "column0", null, 0);
        final Column column11 = bar1.addColumn(store, "column1", null, 1);
        final Column column12 = bar1.addColumn(store, "column2", null, 2);
        final Table bar2 = schema.addTable(store, "bar2", null, mock(Location.class));
        final Column column20 = bar2.addColumn(store, "column0", null, 0);
        final Column column21 = bar2.addColumn(store, "column1", null, 1);
        final Table bar3 = schema.addTable(store, "bar3", null, mock(Location.class));
        final Column column30 = bar3.addColumn(store, "column0", null, 0);
        final Column column31 = bar3.addColumn(store, "column1", null, 1);

        ConstraintCollection constraint = store.createConstraintCollection(null, schema);
        ColumnStatistics columnStatisticsConstraint = new ColumnStatistics(column10.getId());
        columnStatisticsConstraint.setNumDistinctValues(5);
        constraint.add(columnStatisticsConstraint);
        columnStatisticsConstraint = new ColumnStatistics(column11.getId());
        columnStatisticsConstraint.setNumDistinctValues(19);
        constraint.add(columnStatisticsConstraint);
        columnStatisticsConstraint = new ColumnStatistics(column12.getId());
        columnStatisticsConstraint.setNumDistinctValues(12);
        constraint.add(columnStatisticsConstraint);
        columnStatisticsConstraint = new ColumnStatistics(column20.getId());
        columnStatisticsConstraint.setNumDistinctValues(8);
        constraint.add(columnStatisticsConstraint);
        columnStatisticsConstraint = new ColumnStatistics(column21.getId());
        columnStatisticsConstraint.setNumDistinctValues(11);
        constraint.add(columnStatisticsConstraint);
        columnStatisticsConstraint = new ColumnStatistics(column30.getId());
        columnStatisticsConstraint.setNumDistinctValues(17);
        constraint.add(columnStatisticsConstraint);
        columnStatisticsConstraint = new ColumnStatistics(column31.getId());
        columnStatisticsConstraint.setNumDistinctValues(20);
        constraint.add(columnStatisticsConstraint);

        List<Instance> instances = new ArrayList<>();
        Instance instance = new Instance(new UnaryForeignKeyCandidate(column10.getId(), column20.getId()));
        instance.setIsForeignKey(Instance.Result.FOREIGN_KEY);
        instances.add(instance);
        instance = new Instance(new UnaryForeignKeyCandidate(column12.getId(), column30.getId()));
        instance.setIsForeignKey(Instance.Result.FOREIGN_KEY);
        instances.add(instance);
        instance = new Instance(new UnaryForeignKeyCandidate(column21.getId(), column11.getId()));
        instance.setIsForeignKey(Instance.Result.NO_FOREIGN_KEY);
        instances.add(instance);
        instance = new Instance(new UnaryForeignKeyCandidate(column21.getId(), column31.getId()));
        instance.setIsForeignKey(Instance.Result.FOREIGN_KEY);
        instances.add(instance);
        instance = new Instance(new UnaryForeignKeyCandidate(column12.getId(), column31.getId()));
        instance.setIsForeignKey(Instance.Result.NO_FOREIGN_KEY);
        instances.add(instance);

        List<Feature> features = new ArrayList<>();
        features.add(new CoverageFeature(constraint));
        features.add(new DependentAndReferencedFeature());
        features.add(new DistinctDependentValuesFeature(constraint));
        features.add(new MultiDependentFeature());
        features.add(new MultiReferencedFeature());

        trainingSet = new Dataset(instances, features);
        trainingSet.buildDatasetStatistics();
        trainingSet.buildFeatureValueDistribution();

        // for test set
        instances = new ArrayList<>();
        instance = new Instance(new UnaryForeignKeyCandidate(column20.getId(), column12.getId()));
        instance.setIsForeignKey(Instance.Result.UNKNOWN);
        instances.add(instance);
        instance = new Instance(new UnaryForeignKeyCandidate(column10.getId(), column31.getId()));
        instance.setIsForeignKey(Instance.Result.UNKNOWN);
        instances.add(instance);

        testSet = new Dataset(instances, features);
        testSet.buildDatasetStatistics();
        testSet.buildFeatureValueDistribution();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {

    }

    @Test
    public void testSetTrainingSet() throws Exception {
        AbstractClassifier classifier = new NaiveBayes();
        classifier.setTrainingset(trainingSet);
        Dataset trainingSet = classifier.getTrainingset();
        assertEquals(this.trainingSet, trainingSet);
    }

    @Test
    public void testSetTestSet() throws Exception {
        AbstractClassifier classifier = new NaiveBayes();
        classifier.setTestset(trainingSet);
        Dataset testSet = classifier.getTestset();
        assertEquals(trainingSet, testSet);
    }

    @Test
    public void testTrain() throws Exception {
        NaiveBayes classifier = new NaiveBayes();
        classifier.setTrainingset(trainingSet);
        classifier.train();
        Map<Instance.Result, Double> prior = classifier.getPriorProbability();
        assertTrue(prior.get(Instance.Result.FOREIGN_KEY)==4.0/7.0);
        assertTrue(prior.get(Instance.Result.NO_FOREIGN_KEY)==3.0/7.0);

        Map<String, Map<Instance.Result, Map<Object, Double>>> likelyhoods = classifier.getLikelyhoods();
        for (Feature feature : trainingSet.getFeatures()) {
            boolean condition = likelyhoods.containsKey(feature.getFeatureName());
            assertTrue(condition);
            if (condition) {
                assertTrue(likelyhoods.get(feature.getFeatureName()).containsKey(Instance.Result.FOREIGN_KEY));
                assertTrue(likelyhoods.get(feature.getFeatureName()).containsKey(Instance.Result.NO_FOREIGN_KEY));
            }
        }
        assertTrue(likelyhoods.get("Coverage").get(Instance.Result.FOREIGN_KEY).get(5.0/8.0)==1.0/3.0);
        assertTrue(likelyhoods.get("Coverage").get(Instance.Result.FOREIGN_KEY).get(12.0/17.0)==1.0/3.0);
        assertTrue(likelyhoods.get("Coverage").get(Instance.Result.FOREIGN_KEY).get(11.0/20.0)==1.0/3.0);
        assertTrue(likelyhoods.get("Coverage").get(Instance.Result.NO_FOREIGN_KEY).get(11.0/19.0)==1.0/2.0);
        assertTrue(likelyhoods.get("Coverage").get(Instance.Result.NO_FOREIGN_KEY).get(12.0/20.0)==1.0/2.0);

        assertTrue(likelyhoods.get("DependentAndReferenced").get(Instance.Result.FOREIGN_KEY).get(0)==1.0);
        assertTrue(likelyhoods.get("DependentAndReferenced").get(Instance.Result.NO_FOREIGN_KEY).get(0)==1.0);

        assertTrue(likelyhoods.get("DistinctDependentValues").get(Instance.Result.FOREIGN_KEY).get(5L)==1.0/3.0);
        assertTrue(likelyhoods.get("DistinctDependentValues").get(Instance.Result.FOREIGN_KEY).get(12L)==1.0/3.0);
        assertTrue(likelyhoods.get("DistinctDependentValues").get(Instance.Result.FOREIGN_KEY).get(11L)==1.0/3.0);
        assertTrue(likelyhoods.get("DistinctDependentValues").get(Instance.Result.NO_FOREIGN_KEY).get(11L)==1.0/2.0);
        assertTrue(likelyhoods.get("DistinctDependentValues").get(Instance.Result.NO_FOREIGN_KEY).get(12L)==1.0/2.0);

        assertTrue(likelyhoods.get("MultiDependent").get(Instance.Result.FOREIGN_KEY).get(1)==2.0/5.0);
        assertTrue(likelyhoods.get("MultiDependent").get(Instance.Result.FOREIGN_KEY).get(2)==3.0/5.0);
        assertTrue(likelyhoods.get("MultiDependent").get(Instance.Result.NO_FOREIGN_KEY).get(2)==1.0);

        assertTrue(likelyhoods.get("MultiReferenced").get(Instance.Result.FOREIGN_KEY).get(1)==3.0/5.0);
        assertTrue(likelyhoods.get("MultiReferenced").get(Instance.Result.FOREIGN_KEY).get(2)==2.0/5.0);
        assertTrue(likelyhoods.get("MultiReferenced").get(Instance.Result.NO_FOREIGN_KEY).get(1)==2.0/4.0);
        assertTrue(likelyhoods.get("MultiReferenced").get(Instance.Result.NO_FOREIGN_KEY).get(2)==2.0/4.0);
    }

    @Test
    public void testPredict() throws Exception {
        NaiveBayes classifier = new NaiveBayes();
        classifier.setTrainingset(trainingSet);
        classifier.train();
        classifier.setTestset(testSet);
        classifier.predict();
        List<Instance> predicted = classifier.getTestset().getDataset();
        assertTrue(predicted.get(0).getIsForeignKey().equals(Instance.Result.NO_FOREIGN_KEY));
        assertTrue(predicted.get(1).getIsForeignKey().equals(Instance.Result.FOREIGN_KEY));
    }
}
