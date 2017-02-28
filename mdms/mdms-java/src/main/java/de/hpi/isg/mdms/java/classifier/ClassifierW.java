package de.hpi.isg.mdms.java.classifier;

import de.hpi.isg.mdms.java.util.WekaConverter;
import de.hpi.isg.mdms.java.util.Dataset;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

import java.io.File;
import java.io.IOException;

/**
 * Created by Fuga on 20/01/2017.
 */
abstract public class ClassifierW {

    protected Dataset trainSet;

    protected Dataset testSet;

    protected WekaConverter wekaConverter;

    protected Instances data;

    protected Instances testData;

    protected AbstractClassifier cls;

    /**
     * Convert the instances into arff format.
     */
    public void convertData() {
        try {
            wekaConverter = new WekaConverter(trainSet, "trainSet");
            wekaConverter.writeDataIntoFile();
            String fileName = wekaConverter.getFileName();
            ArffLoader loader = new ArffLoader();
            loader.setFile(new File(fileName));
            data = loader.getDataSet();
            data.setClassIndex(data.numAttributes()-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void convertTrainAndTestData() {
        convertData();
        try {
            wekaConverter = new WekaConverter(testSet, "testSet");
            wekaConverter.writeDataIntoFile();
            String fileName = wekaConverter.getFileName();
            ArffLoader loader = new ArffLoader();
            loader.setFile(new File(fileName));
            testData = loader.getDataSet();
            testData.setClassIndex(data.numAttributes()-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract public Classifier buildClassifier() throws Exception;
}
