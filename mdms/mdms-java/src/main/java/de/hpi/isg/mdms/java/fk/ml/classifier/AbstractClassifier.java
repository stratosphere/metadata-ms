package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;

abstract public class AbstractClassifier {

    protected Dataset trainingset;

    protected Dataset testset;

    abstract public void train();

    abstract public void predict();
}
