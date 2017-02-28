package de.hpi.isg.mdms.java.classifier;

import de.hpi.isg.mdms.java.util.Dataset;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;

/**
 * Created by Fuga on 19/01/2017.
 */
public class NaiveBayesW extends ClassifierW {

    public NaiveBayesW(Dataset dataset) {
        this.trainSet = dataset;
        convertData();
    }

    public NaiveBayesW(Dataset trainSet, Dataset testSet) {
        this.trainSet = trainSet;
        this.testSet = testSet;
        convertTrainAndTestData();
    }

    @Override
    public Classifier buildClassifier() throws Exception {
//        Evaluation eval = new Evaluation(data);
//        cls = new NaiveBayes();
//        cls.buildClassifier(data);
//        NaiveBayes naiveBayes = new NaiveBayes();
//        eval.crossValidateModel(naiveBayes, data, 10, new Random(1));
//        System.out.println(eval.toSummaryString("\nNaiveBayes Results\n", false));
//        System.out.println(eval.toClassDetailsString("\nNaiveBayes Results\n"));
//        System.out.println(eval.toMatrixString("\nNaive Bayes Confusing Matrix\n"));

//        AbstractClassifier clf = new NaiveBayes();
        Classifier m_classifier = new NaiveBayes();
        m_classifier.buildClassifier(data);
        Evaluation evaluation = new Evaluation(data);
        evaluation.evaluateModel(m_classifier, testData);
//        evaluation.evaluateModel(clf, testData);
        System.out.println(evaluation.toSummaryString("\nNaiveBayes Results\n", false));
        System.out.println(evaluation.toClassDetailsString("\nNaiveBayes Results\n"));
        System.out.println(evaluation.toMatrixString("\nNaive Bayes Confusing Matrix\n"));
        return m_classifier;
    }
}
