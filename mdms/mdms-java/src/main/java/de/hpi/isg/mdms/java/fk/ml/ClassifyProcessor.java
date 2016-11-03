package de.hpi.isg.mdms.java.fk.ml;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.ml.classifier.Classifier;

/**
 */
public class ClassifyProcessor {

    private Classifier classifier;

    private Dataset dataset;

    public ClassifyProcessor(Classifier classifier, Dataset dataset) {
        this.classifier = classifier;
        this.dataset = dataset;
    }

    public void executeClassifying() {


    }
}
