package de.hpi.isg.mdms.java.util;

import de.hpi.isg.mdms.java.feature.Feature;
import org.apache.commons.collections.ListUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

/**
 * Represent a trainSet holding a list of {@link Instance}
 * @author Lan Jiang
 */
public class Dataset {

    List<Instance> dataset;

    /**
     * Number of classes in this trainSet.
     */
    private long numOfClasses;

    /**
     * Number of instances in this trainSet.
     */
    private long numOfInstance;

    /**
     * The features used in this trainSet.
     */
    List<Feature> features;

    private Map<Instance.Result, List<Instance>> instancesByClasses;

    public Dataset(List<Instance> dataset, List<Feature> features) {
        this.dataset = dataset;
        this.features = features;

        buildDatasetStatistics();
        buildFeatureValueDistribution();
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
     * Calculate the statistics information of this trainSet, i.e., number of classes and instances in this trainSet.
     */
    public void buildDatasetStatistics() {
        numOfInstance = dataset.stream().count();

        instancesByClasses = dataset.stream().collect(Collectors.groupingBy(Instance::getIsForeignKey));
        numOfClasses = instancesByClasses.entrySet().stream().count();
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

    public void labelDataset(Set<UnaryForeignKeyCandidate> foreignKeySet) {
        for (Instance instance : dataset) {
            if (foreignKeySet.contains(instance.getForeignKeyCandidate())) {
                instance.setIsForeignKey(Instance.Result.FOREIGN_KEY);
            }
            else instance.setIsForeignKey(Instance.Result.NO_FOREIGN_KEY);
        }
    }

    public Dataset sampledDataset(Instance.Result result, double ratio) {
        Map<Instance.Result, List<Instance>> instanceByClasses = dataset.stream().collect(Collectors.groupingBy(Instance::getIsForeignKey));
        int reducedSize = (int) ((double)dataset.size()*ratio);
        List<Instance> reducedInstances = new LinkedList<>();
        for (Instance.Result iterResult : Instance.Result.values()) {
            if (iterResult.equals(Instance.Result.UNKNOWN)) {
                continue;
            }
            List<Instance> instances = instanceByClasses.get(iterResult);
            if (iterResult.equals(result)) {
                Collections.shuffle(instances);
                reducedInstances.addAll(instances.subList(0, reducedSize));
            } else {
                reducedInstances.addAll(instanceByClasses.get(iterResult));
            }
        }
        Collections.shuffle(reducedInstances);
        return new Dataset(reducedInstances, this.getFeatures());
    }

    public void normalize() {
        features.stream().forEach(feature -> {
            if (feature.getFeatureType().equals(Feature.FeatureType.Numeric)) {
                OptionalDouble optionalDouble = dataset.stream().flatMapToDouble(instance -> {
                    return DoubleStream.of(
                            Double.parseDouble(instance.getFeatureVector().get(feature.getFeatureName()).toString()));
                }).max();
                if (optionalDouble.isPresent()) {
                    dataset.stream().forEach(instance -> {
                        double value = Double.parseDouble(instance.getFeatureVector()
                                .get(feature.getFeatureName()).toString());
                        instance.getFeatureVector().put(feature.getFeatureName(), value/optionalDouble.getAsDouble());
                    });
                }
            }
        });
    }

    public Dataset getTrainAndReturnTest(double ratio) {
        instancesByClasses = dataset.stream().collect(Collectors.groupingBy(Instance::getIsForeignKey));
        List<Instance> trainInstances = new LinkedList<>();
        List<Instance> testInstances = new LinkedList<>();
        for (Instance.Result result : Instance.Result.values()) {
            if (result.equals(Instance.Result.UNKNOWN))
                continue;
            List<Instance> partialInstances = instancesByClasses.get(result);
            int reducedSize = (int) ((double)partialInstances.size()*ratio);
            Collections.shuffle(partialInstances);
            trainInstances.addAll(partialInstances.subList(0, reducedSize));
            testInstances.addAll(partialInstances.subList(reducedSize, partialInstances.size()));
        }
        this.dataset = trainInstances;
        buildDatasetStatistics();

        Dataset testData = new Dataset(testInstances, this.getFeatures());
        return testData;
    }

    public Map<Instance.Result, List<Instance>> getInstancesByClasses() {
        return instancesByClasses;
    }

    public void setLabel(Instance.Result result) {
        this.dataset.stream().forEach(instance -> instance.setIsForeignKey(result));
    }

    public Dataset combineWith(Dataset minorityDataset) {
        List<Instance> instances = ListUtils.union(this.dataset, minorityDataset.getDataset());
        return new Dataset(instances, minorityDataset.getFeatures());
    }
}
