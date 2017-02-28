package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 09/02/2017
 */
public class RandomUnderSampling extends UnderSampling {

    public RandomUnderSampling(Dataset dataset, Instance.Result majorityClass, double ratio) {
        super(dataset, majorityClass, ratio);
    }

    public RandomUnderSampling(Dataset dataset, Instance.Result majorityClass, Instance.Result minorityClass, double ratio) {
        super(dataset, majorityClass, minorityClass, ratio);
    }

    @Override
    public Dataset sampling() {
        List<Instance> instances = dataset.getDataset();
        instanceByClasses = dataset.getInstancesByClasses();
        int reducedSize = (int) ((double)instances.size()*ratio);
        List<Instance> reducedInstances = new LinkedList<>();
        List<Instance> ins = instanceByClasses.get(majorityClass);
        Collections.shuffle(ins);
        reducedInstances.addAll(ins.subList(0, reducedSize));
        Collections.shuffle(reducedInstances);
        Dataset sampledDataset = new Dataset(reducedInstances, this.dataset.getFeatures());
        return sampledDataset;
    }
}
