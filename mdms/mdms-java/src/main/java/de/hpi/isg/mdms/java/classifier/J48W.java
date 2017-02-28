package de.hpi.isg.mdms.java.classifier;

import de.hpi.isg.mdms.java.util.Dataset;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;

/**
 * Created by Fuga on 20/01/2017.
 */
public class J48W extends ClassifierW {

    public J48W(Dataset dataset) {
        this.trainSet = dataset;
        convertData();
    }

    public J48W(Dataset trainSet, Dataset testSet) {
        this.trainSet = trainSet;
        this.testSet = testSet;
        convertTrainAndTestData();
    }

    @Override
    public Classifier buildClassifier() throws Exception {
//        Evaluation eval = new Evaluation(data);
//
//        J48 tree = new J48();
//        eval.crossValidateModel(tree, data, 10, new Random(1));
//        System.out.println(eval.toSummaryString("\nJ48 Results\n", false));
//        System.out.println(eval.toClassDetailsString("\nJ48 Results\n"));
//        System.out.println(eval.toMatrixString("\nJ48 Confusing Matrix\n"));

        Classifier m_classifier = new J48();
        m_classifier.buildClassifier(data);
        Evaluation evaluation = new Evaluation(data);
        evaluation.evaluateModel(m_classifier, testData);
        System.out.println(evaluation.toSummaryString("\nJ48 Results\n", false));
        System.out.println(evaluation.toClassDetailsString("\nJ48 Results\n"));
        System.out.println(evaluation.toMatrixString("\nJ48 Confusing Matrix\n"));
        return m_classifier;
    }
}
