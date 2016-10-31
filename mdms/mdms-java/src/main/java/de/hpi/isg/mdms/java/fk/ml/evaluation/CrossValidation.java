package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.java.fk.ml.classifier.AbstractClassifier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Lan Jiang
 */
public class CrossValidation {

    private Dataset dataset;

    private AbstractClassifier classifier;

    private List<Dataset> partialDatasets;

    /**
     * The f measure score of the classifier on this dataset with cross validation.
     */
    private double fscore = 0.0;

    /**
     * Number of folds in this cross validation. The default value is 10.
     */
    private int numFolds = 10;

    public CrossValidation(Dataset dataset, AbstractClassifier classifier) {
        this.dataset = dataset;
        this.classifier = classifier;
    }

    public CrossValidation(Dataset dataset, AbstractClassifier classifier, int numFolds) {
        this.dataset = dataset;
        this.classifier = classifier;
        this.numFolds = numFolds;
    }

    private void execute() {
        ClassifierEvaluation evaluation = new FMeasureEvaluation(Instance.Result.FOREIGN_KEY);
        seperateDataset();

        for (int i = 0; i< numFolds; i++) {
            Dataset testset = partialDatasets.get(i);
            testset.buildDatasetStatistics();
            testset.buildFeatureValueDistribution();
            classifier.setTestset(testset);
            Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth = new HashMap<>();
            testset.getDataset().stream()
                    .forEach(instance -> groundTruth.putIfAbsent(instance.getForeignKeyCandidate(), instance.getIsForeignKey()));
            evaluation.setGroundTruth(groundTruth);

            dataset.removeTestset(testset);
            Dataset trainingset = dataset;
            trainingset.buildDatasetStatistics();
            trainingset.buildFeatureValueDistribution();

            classifier.setTrainingset(trainingset);
            classifier.train();
            classifier.predict();
            Map<UnaryForeignKeyCandidate, Instance.Result> predicted = new HashMap<>();
            classifier.getTestset().getDataset().stream()
                    .forEach(instance -> predicted.putIfAbsent(instance.getForeignKeyCandidate(), instance.getIsForeignKey()));
            evaluation.setPredicted(predicted);
            evaluation.evaluate();
            fscore += (double) evaluation.getEvaluation();
        }
        fscore /= numFolds;
    }

    private void seperateDataset() {
        long numInstance = dataset.getNumOfInstance();
        int partialDatasetSize = (int) (numInstance / numFolds);
//        int reminder = (int) (numInstance % numFolds);
        for (int i = 0; i<numFolds; i++) {
            List<Instance> partialInstances = null;
            if (i!=numFolds-1) {
                partialInstances = dataset.getDataset().subList(i*partialDatasetSize, (i+1)*partialDatasetSize);
            } else {
                partialInstances = dataset.getDataset().subList(i*partialDatasetSize, dataset.getDataset().size());
            }
            Dataset partialDataset = new Dataset(partialInstances, dataset.getFeatures());
            partialDatasets.add(partialDataset);
        }
    }
}
