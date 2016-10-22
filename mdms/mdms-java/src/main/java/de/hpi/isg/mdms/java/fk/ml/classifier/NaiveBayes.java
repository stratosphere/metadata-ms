package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;

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

    public NaiveBayes(Dataset dataset) {
        priorProbability = new HashMap<>();
    }

    private void calcultePriorProbability() {
        Map<Instance.Result, List<Instance>> instancesByClasses = trainingset.getDataset().stream()
                .collect(Collectors.groupingBy(Instance::getIsForeignKey));
        instancesByClasses.entrySet().stream().forEach(entry -> {
            double prior = (entry.getValue().size() + 1.0) / (trainingset.getDataset().size() + 1.0 * trainingset.getNumOfClasses());
            priorProbability.put(entry.getKey(), prior);
        });
    }


    @Override
    public void train() {
        calcultePriorProbability();
    }

    @Override
    public void predict() {

    }
}
