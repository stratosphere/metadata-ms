package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
//    private Map<Instance.Result, Map<String, Map<Object, Double>>> likelyhoods;
    private List<FeatureValue> likelyhoods;

    public NaiveBayes(Dataset dataset) {
        this.trainingset = dataset;
        priorProbability = new HashMap<>();
        likelyhoods = new ArrayList<>();
    }

    private void calcultePriorProbability(Map<Instance.Result, List<Instance>> instancesByClasses) {
        instancesByClasses.entrySet().stream().forEach(entry -> {
            double prior = (entry.getValue().size() + 1.0) / (trainingset.getDataset().size() + 1.0 * trainingset.getNumOfClasses());
            priorProbability.put(entry.getKey(), prior);
        });
    }

    private void calculateLikelyhoods(Map<Instance.Result, List<Instance>> instancesByClasses) {
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
                likelyhoods.add(new FeatureValue(featureName, entry.getKey(), partialfeatureValue));
            });
        });
    }

    @Override
    public void train() {
        Map<Instance.Result, List<Instance>> instancesByClasses = trainingset.getDataset().stream()
                .collect(Collectors.groupingBy(Instance::getIsForeignKey));
        calcultePriorProbability(instancesByClasses);
        calculateLikelyhoods(instancesByClasses);
    }

    @Override
    public void predict() {

    }

    public class FeatureValue {
        private String featureName;

        private Instance.Result label;

        private Map<Object, Double> valueCount;

        public FeatureValue(String featureName, Instance.Result label, Map<Object, Double> valueCount) {
            this.featureName = featureName;
            this.label = label;
            this.valueCount = valueCount;
        }

        public String getFeatureName() {
            return featureName;
        }

        public Instance.Result getLabel() {
            return label;
        }

        public Map<Object, Double> getValueCount() {
            return valueCount;
        }
    }
}
