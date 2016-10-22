package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.Map;

abstract public class AbstractClassifier {

    protected Dataset trainingset;

    protected Dataset testset;

    abstract public void train();

    abstract public Map<UnaryForeignKeyCandidate, Instance.Result> predict();
}
