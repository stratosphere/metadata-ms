package de.hpi.isg.mdms.java.classifier;

import de.hpi.isg.mdms.java.util.Dataset;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.SMO;

/**
 * Created by Fuga on 20/01/2017.
 */
public class SVMW extends ClassifierW {

    public SVMW(Dataset dataset) {
        this.trainSet = dataset;
        try {
            convertData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public SVMW(Dataset trainSet, Dataset testSet) {
        this.trainSet = trainSet;
        this.testSet = testSet;
        convertTrainAndTestData();
    }

    @Override
    public Classifier buildClassifier() throws Exception {
        Evaluation eval = new Evaluation(data);

//        SMO svm = new SMO();
//        eval.crossValidateModel(svm, data, 10, new Random(1));
//        System.out.println(eval.toSummaryString("\nSVM Results\n", false));
//        System.out.println(eval.toClassDetailsString("\nSVM Results\n"));
//        System.out.println(eval.toMatrixString("\nSVM Confusing Matrix\n"));

        Classifier m_classifier = new SMO();
        m_classifier.buildClassifier(data);
        Evaluation evaluation = new Evaluation(data);
        evaluation.evaluateModel(m_classifier, testData);
        System.out.println(evaluation.toSummaryString("\nSVM Results\n", false));
        System.out.println(evaluation.toClassDetailsString("\nSVM Results\n"));
        System.out.println(evaluation.toMatrixString("\nSVM Confusing Matrix\n"));
        return m_classifier;
    }
}
