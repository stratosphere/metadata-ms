package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;

import java.util.List;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 09/02/2017
 */
abstract public class NonrandomUnderSampling extends UnderSampling {

    /**
     * The possiblity to be sampled for each {@link Instance}
     */
    protected Map<Instance, Double> possiblityOfSelected;

    /**
     * Containing {@link Instance}s that are not selected in the whole sampling process.
     */
    protected List<Instance> unselectedMajorityClassInstances;

    /**
     * Distribute and accumulate the possibility of selecting each sample into the range of 0 and 1,
     * allocates each sample a slot.
     */
    protected Map<Double, Instance> samplingSlot;

    public NonrandomUnderSampling(Dataset dataset, Instance.Result majorityClass, double ratio) {
        super(dataset, majorityClass, ratio);
    }

    public NonrandomUnderSampling(Dataset dataset, Instance.Result majorityClass, Instance.Result minorityClass,
                                  double ratio) {
        super(dataset, majorityClass, minorityClass, ratio);
    }


}
