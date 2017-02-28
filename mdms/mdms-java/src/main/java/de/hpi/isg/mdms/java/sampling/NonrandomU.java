package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 06/02/2017
 */
public class NonrandomU extends UnderSampling {

    /**
     * The nearest distance of all the samples in the majority class.
     */
    private Map<Instance, Double> nearestDistance;

    private Map<Instance, Double> possiblityOfSelected;

    private Map<Double, Instance> samplingSlot;

    private List<Instance> unselectedMajorityClassInstances;

    private int N;

    public NonrandomU(Dataset dataset, Instance.Result majorityClass, int N) {
        this.dataset = dataset;
        this.majorityClass = majorityClass;
        this.N = N;
        instanceByClasses = this.dataset.getDataset()
                .stream().collect(Collectors.groupingBy(Instance::getIsForeignKey));
        nearestDistance = new HashMap<>();
    }

    public Dataset sampling(double ratio) {
        int reducedSize = (int) ((double)dataset.getDataset().size()*ratio);
        calculateNearestDistance();
        calculateSampledPossibility();
        List<Instance> instances = new LinkedList<>();
        Random random = new Random();
        for (int i = 0; i < reducedSize; i++) {
            double rand = random.nextDouble();
            double point = search(samplingSlot.keySet(), rand);
            if (point!=-1.0) {
                instances.add(samplingSlot.get(point));
            }
        }

//        instanceByClasses.get(majorityClass).removeAll(instances);
        unselectedMajorityClassInstances = instanceByClasses.get(majorityClass);

        Instance.Result minorityClass = majorityClass== Instance.Result.FOREIGN_KEY? Instance.Result.NO_FOREIGN_KEY: Instance.Result.FOREIGN_KEY;
        instances.addAll(dataset.getDataset().stream().filter(instance -> instance.getIsForeignKey()==minorityClass).collect(Collectors.toList()));
        Collections.shuffle(instances);
        return new Dataset(instances, this.dataset.getFeatures());
    }

    private void calculateSampledPossibility() {
        possiblityOfSelected = new HashMap<>();
        nearestDistance.entrySet().stream()
                .forEach(entry -> possiblityOfSelected.putIfAbsent(entry.getKey(), 1/entry.getValue()));
        double sum = possiblityOfSelected.values().stream().mapToDouble(Double::doubleValue).sum();
        possiblityOfSelected.entrySet().stream()
                .forEach(entry -> possiblityOfSelected.put(entry.getKey(), entry.getValue()/sum));
        double point = 0.0;
        samplingSlot = new TreeMap<>();
        for (Map.Entry<Instance, Double> entry : possiblityOfSelected.entrySet()) {
            point += entry.getValue();
            samplingSlot.putIfAbsent(point, entry.getKey());
        }
    }

    private void calculateNearestDistance() {
        List<Instance> minority = instanceByClasses.get(majorityClass == Instance.Result.FOREIGN_KEY?
                Instance.Result.NO_FOREIGN_KEY: Instance.Result.FOREIGN_KEY);
        for (Instance instance : instanceByClasses.get(majorityClass)) {
            double distance = shortestDistance(minority, instance);
            if (distance==0) {
//                distance += Double.MIN_NORMAL;
                continue;
            }
            nearestDistance.putIfAbsent(instance, distance);
        }
    }

    private double shortestDistance(List<Instance> minority, Instance instance) {
        double shortest = Double.MAX_VALUE;
        List<Double> distances = new LinkedList<>();
        for (Instance inst : minority) {
//            double distance = distance(inst, instance);
//            if (distance<shortest) {
//                shortest = distance;
//            }
            distances.add(distance(inst, instance));
        }
        Collections.sort(distances);
        return distances.subList(0, N).stream().mapToDouble(value -> value).average().getAsDouble();
//        return shortest;
    }

    private double distance(Instance inst1, Instance inst2) {
        double sum = 0.0;
        for (String feature : inst1.getFeatureVector().keySet()) {
            double value1 = Double.parseDouble(inst1.getFeatureVector().get(feature).toString());
            double value2 = Double.parseDouble(inst2.getFeatureVector().get(feature).toString());
            sum += Math.pow(value1-value2,2);
        }
        return Math.sqrt(sum);
    }

    private double search(Set<Double> list, double number) {
//        final OptionalDouble optionalDouble = list.stream().flatMapToDouble(value -> {
//            if (value>=number) {
//                return DoubleStream.of(value);
//            }
//            else {
//                return DoubleStream.of(Double.MAX_VALUE);
//            }
//        }).min();
//        final Optional<Double> min = list.stream().filter(value -> value>= number).reduce(Double::min);
//        List<Double> sublist = list.stream().filter(value -> value >= number).sorted().collect(Collectors.toList());
//        double min = sublist.get(0);
//        return min;
        for (double value : list) {
            if (value>=number) {
                return value;
            }
        }
        return -1.0;
    }

    public List<Instance> getUnselectedMajorityClassInstances() {
        return unselectedMajorityClassInstances;
    }

    @Override
    public Dataset sampling() {
        return null;
    }
}
