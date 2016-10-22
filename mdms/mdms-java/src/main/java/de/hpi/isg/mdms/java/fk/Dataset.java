package de.hpi.isg.mdms.java.fk;

import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Dataset {

    List<Instance> dataset;

    /**
     * Number of classes in this dataset.
     */
    private long numOfClasses;

    /**
     * Number of instances in this dataset.
     */
    private long numOfInstance;

    /**
     * The features used in this dataset.
     */
    List<Feature> features;

//    /**
//     * Store the value distribution in each feature, in the form of <FeatureName, <Value, Count>>
//     */
//    private Map<String, Map<Object, Double>> featureValueDistribution;

    public Dataset(List<Instance> dataset, List<Feature> features) {
        this.dataset = dataset;
        this.features = features;
//        featureValueDistribution = new HashMap<>();
    }

    public List<Instance> getDataset() {
        return dataset;
    }

//    public Map<String, Map<Object, Double>> getFeatureValueDistribution() {
//        return featureValueDistribution;
//    }

    public long getNumOfClasses() {
        return numOfClasses;
    }

    public long getNumOfInstance() {
        return numOfInstance;
    }

    public List<Feature> getFeatures() {
        return features;
    }

    /**
     * Calculate the statistics information of this dataset, i.e., number of classes and instances in this dataset.
     */
    public void buildDatasetStatistics() {
        numOfInstance = dataset.stream().count();

        Map<Instance.Result, List<Instance>> instanceByClasses = dataset.stream().collect(Collectors.groupingBy(Instance::getIsForeignKey));
        numOfClasses = instanceByClasses.entrySet().stream().count();
    }

    public void buildFeatureValueDistribution() {
        features.stream().forEach(feature -> feature.calcualteFeatureValue(dataset));
//        features.forEach(feature -> feature.calculateFeatureValueDistribution(this));
    }
}
