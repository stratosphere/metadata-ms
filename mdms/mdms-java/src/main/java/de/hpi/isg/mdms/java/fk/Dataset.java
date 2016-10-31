package de.hpi.isg.mdms.java.fk;

import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represent a dataset holding a list of {@link Instance}
 * @author Lan Jiang
 */
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

    public Dataset(List<Instance> dataset, List<Feature> features) {
        this.dataset = dataset;
        this.features = features;
    }

    public List<Instance> getDataset() {
        return dataset;
    }

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
    }

    public Instance getInstanceByUnaryTuple(UnaryForeignKeyCandidate unaryForeignKeyCandidate) {
        Optional<Instance> result = this.dataset.stream()
                .filter(instance -> instance.getForeignKeyCandidate().equals(unaryForeignKeyCandidate))
                .findFirst();
        return result.orElse(null);
    }

    public void removeTestset(Dataset testset) {
        dataset.removeAll(testset.dataset);
        buildDatasetStatistics();
        buildFeatureValueDistribution();
    }
}
