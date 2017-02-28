package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.classifier.AdaboostW;
import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;
import weka.classifiers.Classifier;
import weka.classifiers.meta.AdaBoostM1;

/**
 * @author Lan Jiang
 * @since 09/02/2017
 */
public class EasyEnsemble extends NonrandomUnderSampling {

    /**
     * The number of subsets to sample from the majority class.
     */
    private int subsetsCount;

    /**
     * The number of iterations to train an AdaBoost ensemble algorithm.
     */
    private int iterationsCount;

    public EasyEnsemble(Dataset dataset, Instance.Result majorityClass, double ratio
            , int subsetsCount, int iterationsCount) {
        super(dataset, majorityClass, ratio);
        this.subsetsCount = subsetsCount;
        this.iterationsCount = iterationsCount;
    }

    public EasyEnsemble(Dataset dataset, Instance.Result majorityClass,
                        Instance.Result minorityClass, double ratio,
                        int subsetsCount, int iterationsCount) {
        super(dataset, majorityClass, minorityClass, ratio);
        this.subsetsCount = subsetsCount;
        this.iterationsCount = iterationsCount;
    }

    public Classifier[] getClassifier() {
        int i = 0;
        RandomUnderSampling randomUnderSampling = new RandomUnderSampling(dataset, majorityClass, ratio);
        Dataset minorityDataset = new Dataset(instanceByClasses.get(minorityClass), dataset.getFeatures());
        minorityDataset.setLabel(minorityClass);
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

    /**
     * It is not useful, so not implemented.
     * @return {@code null}
     */
    @Override
    public Dataset sampling() {
        return null;
    }
}
