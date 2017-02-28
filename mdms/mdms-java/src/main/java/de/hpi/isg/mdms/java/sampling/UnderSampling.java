package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The abstract base class for all the under sampling methods.
 * @author Lan Jiang
 * @since 09/02/2017
 */
abstract public class UnderSampling {

    /**
     * The {@link Dataset} that need the under sampling.
     */
    protected Dataset dataset;

    /**
     * Pointing out the majority and minority classes in this {@link Dataset}.
     */
    protected Instance.Result majorityClass;
    protected Instance.Result minorityClass;

    /**
     * The ratio of majority class samples that the sampling method should keep.
     */
    protected double ratio;

    /**
     * Split the {@link Dataset} by the label information.
     */
    protected Map<Instance.Result, List<Instance>> instanceByClasses;

    public UnderSampling(Dataset dataset, Instance.Result majorityClass, double ratio) {
        this.dataset = dataset;
        this.majorityClass = majorityClass;
        this.ratio = ratio;
        this.minorityClass = (majorityClass== Instance.Result.NO_FOREIGN_KEY) ?
                Instance.Result.FOREIGN_KEY: Instance.Result.NO_FOREIGN_KEY;
        instanceByClasses = this.dataset.getDataset()
                .stream().collect(Collectors.groupingBy(Instance::getIsForeignKey));
    }

    public UnderSampling(Dataset dataset, Instance.Result majorityClass, Instance.Result minorityClass,
                         double ratio) {
        this(dataset, majorityClass, ratio);
        this.minorityClass = minorityClass;
    }

    protected UnderSampling() {
    }

    /**
     * The sampling method, after sampling creates a new {@link Dataset} containing all the {@link Instance} sampled.
     * @return a {@link Dataset} containing all the sampled {@link Instance}
     */
    abstract public Dataset sampling();
}
