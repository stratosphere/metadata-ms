package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.List;
import java.util.Map;

/**
 * An abstract classifier class holding some basic necessary information of a classifier. It has to be extended by a
 * concrete classifier, which overrides the <method>train</method> and <method>predict</method> methods.
 * @author Lan Jiang
 */
abstract public class AbstractClassifier implements Classifier {

    protected Dataset trainingset;

    protected Dataset testset;

    public void setTrainingset(Dataset trainingset) {
        this.trainingset = trainingset;
    }

    public void setTestset(Dataset testset) {
        this.testset = testset;
    }

    public Dataset getTrainingset() {
        return trainingset;
    }

    public Dataset getTestset() {
        return testset;
    }
}
