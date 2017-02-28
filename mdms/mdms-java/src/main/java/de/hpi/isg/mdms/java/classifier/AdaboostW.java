package de.hpi.isg.mdms.java.classifier;

import de.hpi.isg.mdms.java.util.Dataset;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.meta.AdaBoostM1;
import weka.classifiers.trees.J48;

/**
 * @author Lan Jiang
 * @since 09/02/2017
 */
public class AdaboostW extends ClassifierW {

    public AdaboostW(Dataset dataset) {
        this.trainSet = dataset;
        convertData();
    }

    public AdaboostW(Dataset trainSet, Dataset testSet) {
        this.trainSet = trainSet;
        this.testSet = testSet;
        convertTrainAndTestData();
    }

    @Override
    public Classifier buildClassifier() {
        Classifier m_classifier = new AdaBoostM1();
//        Evaluation evaluation = null;
        try {
            m_classifier.buildClassifier(data);
//            evaluation = new Evaluation(data);
//            evaluation.evaluateModel(m_classifier, testData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return m_classifier;
    }
}
