package de.hpi.isg.mdms.java.classifier;

import de.hpi.isg.mdms.java.util.Dataset;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.rules.DecisionTable;

/**
 * Created by Fuga on 20/01/2017.
 */
public class DecisionTableW extends ClassifierW{

    public DecisionTableW(Dataset dataset) {
        this.trainSet = dataset;
        try {
            convertData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DecisionTableW(Dataset trainSet, Dataset testSet) {
        this.trainSet = trainSet;
        this.testSet = testSet;
        convertTrainAndTestData();
    }

    @Override
    public Classifier buildClassifier() throws Exception {
//        Evaluation eval = new Evaluation(data);
//
//        DecisionTable decisionTable = new DecisionTable();
//        eval.crossValidateModel(decisionTable, data, 10, new Random(1));
//        System.out.println(eval.toSummaryString("\nDecisionTable Results\n", false));
//        System.out.println(eval.toClassDetailsString("\nDecisionTable Results\n"));
//        System.out.println(eval.toMatrixString("\nDecision Table Confusing Matrix\n"));

        Classifier m_classifier = new DecisionTable();
        m_classifier.buildClassifier(data);
        Evaluation evaluation = new Evaluation(data);
        evaluation.evaluateModel(m_classifier, testData);
        System.out.println(evaluation.toSummaryString("\nDecisionTable Results\n", false));
        System.out.println(evaluation.toClassDetailsString("\nDecisionTable Results\n"));
        System.out.println(evaluation.toMatrixString("\nDecisionTable Confusing Matrix\n"));
        return m_classifier;
    }
}
