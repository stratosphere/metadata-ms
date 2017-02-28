package de.hpi.isg.mdms.java.feature;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.java.util.Instance;
import de.hpi.isg.mdms.java.util.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class DistinctDependentValuesFeature extends Feature {

    private final static String DISTINCT_DEPENDENT_VALUES_FEATURE_NAME = "distinct_dependent_values";

    /**
     * Stores the distinct value counts for all columns.
     */
    private final Int2LongMap distinctValues;

    /**
     * Stores the number of inds.
     */
    private int numINDs;

    public DistinctDependentValuesFeature() {
        distinctValues = new Int2LongOpenHashMap();
    }

    public DistinctDependentValuesFeature(ConstraintCollection columnStatsConstraintCollection) {
        featureName = DISTINCT_DEPENDENT_VALUES_FEATURE_NAME;
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
        numINDs = instanceCollection.size();
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();

            long depDistinctValueCount = this.distinctValues.get(fkc.getDependentColumnId());
            double nomalized = normalize(depDistinctValueCount);
            instance.getFeatureVector().put(DISTINCT_DEPENDENT_VALUES_FEATURE_NAME, nomalized);
        }
    }

    @Override
    public double normalize(double valueForNormalizing) {
        return valueForNormalizing/(double)numINDs;
    }
}
