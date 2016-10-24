package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.java.fk.feature.Feature;
import it.unimi.dsi.fastutil.Hash;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jianghm on 2016/10/22.
 */
public class NaiveBayes extends AbstractClassifier {

    /**
     * Indicate the prior probability, i.e. p(c)
     */
    private Map<Instance.Result, Double> priorProbability;

    /**
     * Indicate the likelyhoods, i.e. p(x|c)
     */
//    private List<FeatureValue> likelyhoods;
    private Map<String, Map<Instance.Result, Map<Object, Double>>> likelyhoods;

    private Map<Instance.Result, List<Instance>> instancesByClasses;

    public NaiveBayes() {
//        this.trainingset = dataset;
        priorProbability = new HashMap<>();
//        likelyhoods = new ArrayList<>();
        likelyhoods = new HashMap<>();
    }

    private void calcultePriorProbability() {
        instancesByClasses.entrySet().stream().forEach(entry -> {
            double prior = (entry.getValue().size() + 1.0) / (trainingset.getDataset().size() + 1.0 * trainingset.getNumOfClasses());
            priorProbability.put(entry.getKey(), prior);
        });
    }

    private void calculateLikelyhoods() {
        List<Feature> features = trainingset.getFeatures();
        features.stream().forEach(feature -> {
            String featureName = feature.getFeatureName();
            instancesByClasses.entrySet().stream().forEach(entry -> {
                Map<Object, Double> partialfeatureValue = new HashMap<>();
                entry.getValue().stream().map(Instance::getFeatureVector).flatMap(map -> map.entrySet().stream())
                        .filter(stringObjectEntry -> stringObjectEntry.getKey().equals(featureName))
                        .forEach(stringObjectEntry -> {
                            Object value = stringObjectEntry.getValue();
                            if (partialfeatureValue.containsKey(value)) {
                                partialfeatureValue.put(value, partialfeatureValue.get(value)+1.0);
                            } else {
                                partialfeatureValue.put(value, 1.0);
                            }
                        });
                partialfeatureValue.entrySet().stream().forEach(pfventry -> {
                    partialfeatureValue.put(pfventry.getKey(), (pfventry.getValue() + 1.0) / (entry.getValue().size() + partialfeatureValue.size()));
                });
                if (likelyhoods.containsKey(featureName)) {
                    likelyhoods.get(featureName).put(entry.getKey(), partialfeatureValue);
                } else {
                    Map<Instance.Result, Map<Object, Double>> featureValueByClass = new HashMap<>();
                    featureValueByClass.put(entry.getKey(), partialfeatureValue);
                    likelyhoods.put(featureName, featureValueByClass);
                }
//                likelyhoods.add(new FeatureValue(featureName, entry.getKey(), partialfeatureValue));
            });
        });
    }

    @Override
    public void train() {
        instancesByClasses = trainingset.getDataset().stream()
                .collect(Collectors.groupingBy(Instance::getIsForeignKey));
        calcultePriorProbability();
        calculateLikelyhoods();
    }

    @Override
    public Map<UnaryForeignKeyCandidate, Instance.Result> predict() {
        Map<UnaryForeignKeyCandidate, Instance.Result> predicted = new HashMap<>();
        testset.getDataset().stream().forEach(instance -> {
            double max = 0.0;
            Instance.Result maxResult = Instance.Result.UNKNOWN;
            for (Instance.Result label : Instance.Result.values()) {
                double result = 1.0;
                for (String feature : instance.getFeatureVector().keySet()) {
                    if (likelyhoods.get(feature).get(label)
                            .containsKey(instance.getFeatureVector().get(feature))) {
                        result *= likelyhoods.get(feature).get(label)
                                .get(instance.getFeatureVector().get(feature));
                    } else {
                        result *= (1.0 / instancesByClasses.get(label).size() + likelyhoods.get(feature).get(label).size());
                    }
                }
                if (result > max) {
                    max = result;
                    maxResult = label;
                }
            }
            predicted.putIfAbsent(instance.getForeignKeyCandidate(), maxResult);
        });
        return predicted;
    }

//    public class FeatureValue {
//        private String featureName;
//
//        private Instance.Result label;
//
//        private Map<Object, Double> valueCount;
//
//        public FeatureValue(String featureName, Instance.Result label, Map<Object, Double> valueCount) {
//            this.featureName = featureName;
//            this.label = label;
//            this.valueCount = valueCount;
//        }
//
//        public String getFeatureName() {
//            return featureName;
//        }
//
//        public Instance.Result getLabel() {
//            return label;
//        }
//
//        public Map<Object, Double> getValueCount() {
//            return valueCount;
//        }
//    }
}
