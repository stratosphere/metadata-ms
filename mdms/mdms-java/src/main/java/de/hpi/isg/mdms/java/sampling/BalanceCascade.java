package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.classifier.AdaboostW;
import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;
import weka.classifiers.Classifier;

/**
 * The implementation of Balance Cascade non random under sampling method.
 * @author Lan Jiang
 * @since 10/02/2017
 */
public class BalanceCascade extends NonrandomUnderSampling implements UnderSamplingForBaseClassifier {

    /**
     * The number of subsets to sample from the majority class.
     */
    private int subsetsCount;

    /**
     * The number of iterations to train an AdaBoost ensemble algorithm.
     */
    private int iterationsCount;

    public BalanceCascade(Dataset dataset, Instance.Result majorityClass, double ratio, int subsetsCount, int iterationsCount) {
        super(dataset, majorityClass, ratio);
        this.subsetsCount = subsetsCount;
        this.iterationsCount = iterationsCount;
    }

    public BalanceCascade(Dataset dataset, Instance.Result majorityClass, Instance.Result minorityClass, double ratio, int subsetsCount, int iterationsCount) {
        super(dataset, majorityClass, minorityClass, ratio);
        this.subsetsCount = subsetsCount;
        this.iterationsCount = iterationsCount;
    }

    @Override
    public Dataset sampling() {
        return null;
    }

    @Override
    public Classifier[] getClassifier() {
        int i = 0;
        RandomUnderSampling randomUnderSampling = new RandomUnderSampling(dataset, majorityClass, ratio);
        Dataset minorityDataset = new Dataset(instanceByClasses.get(minorityClass), dataset.getFeatures());
        minorityDataset.setLabel(minorityClass);
        double fprate = (double)minorityDataset.getDataset().size()/(double)instanceByClasses.get(majorityClass).size();
        fprate = Math.pow(fprate, 1/(subsetsCount-1));

        Classifier[] classifiers = new Classifier[subsetsCount];
        while ( i < subsetsCount ) {
            Dataset reducedMajorityDataset = randomUnderSampling.sampling();
            Dataset reducedDataset = reducedMajorityDataset.combineWith(minorityDataset);

            AdaboostW adaboostW = new AdaboostW(reducedDataset);
            Classifier classifier = adaboostW.buildClassifier();
            classifiers[i] = classifier;

            i++;
        }
        return classifiers;
    }
}
