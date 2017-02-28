package de.hpi.isg.mdms.java.feature;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.java.util.Instance;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/22.
 */
public class CoverageFeature extends Feature {

    private final static String COVERAGE_FEATURE_NAME = "coverage";

    /**
     * Stores the distinct value counts for all columns.
     */
    private final Int2LongMap distinctValues;

    public CoverageFeature() {
        distinctValues = new Int2LongOpenHashMap();
    }

    public CoverageFeature(ConstraintCollection columnStatsConstraintCollection) {
        featureName = COVERAGE_FEATURE_NAME;
        featureType = FeatureType.Numeric;

        // Initialize the distinct value counts.
        this.distinctValues = new Int2LongOpenHashMap((int) columnStatsConstraintCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof ColumnStatistics).count());
        columnStatsConstraintCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof ColumnStatistics)
                .map(constraint -> (ColumnStatistics) constraint)
                .forEach(distinctValueCount -> distinctValues.put(
                        distinctValueCount.getTargetReference().getTargetId(),
                        distinctValueCount.getNumDistinctValues()
                ));
    }

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        for (Instance instance : instanceCollection) {
            int depColumnId = instance.getForeignKeyCandidate().getDependentColumnId();
            int refColumnId = instance.getForeignKeyCandidate().getReferencedColumnId();

            double depDistinctValueCount = this.distinctValues.get(depColumnId);
            double refDistinctValueCount = this.distinctValues.get(refColumnId);
            // for smooth
            if (refDistinctValueCount==0) {
                refDistinctValueCount = 1;
            }

            double coverage = depDistinctValueCount/refDistinctValueCount;
            instance.getFeatureVector().put(featureName, coverage);
        }
    }

    @Override
    public double normalize(double valueForNormalizing) {
        // coverage needs no normalization.
        return 0.0;
    }


//    @Override
//    public void calculateFeatureValueDistribution(Dataset trainSet) {
//        // count for each value in this feature, just for nominal type feature
//        Map<Object, Double> eachValueCount = new HashMap<>();
//
//        for (Instance instance : trainSet.getDataset()) {
//            int depColumnId = instance.getForeignKeyCandidate().getDependentColumnId();
//            int refColumnId = instance.getForeignKeyCandidate().getReferencedColumnId();
//
//            double depDistinctValueCount = this.distinctValues.get(depColumnId);
//            double refDistinctValueCount = this.distinctValues.get(refColumnId);
//
//            double coverage = depDistinctValueCount/refDistinctValueCount;
//            if (eachValueCount.containsKey(coverage)) {
//                eachValueCount.put(coverage, eachValueCount.get(coverage)+1);
//            } else {
//                eachValueCount.put(coverage, 1.0);
//            }
//        }
//        trainSet.getFeatureValueDistribution().put(featureName, eachValueCount);
//    }
}
