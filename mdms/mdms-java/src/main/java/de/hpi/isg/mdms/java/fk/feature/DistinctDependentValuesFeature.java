package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class DistinctDependentValuesFeature extends Feature {

    private final static String DISTINCT_DEPENDENT_VALUES_FEATURE_NAME = "DistinctDependentValues";

    /**
     * Stores the distinct value counts for all columns.
     */
    private final Int2LongMap distinctValues;

    public DistinctDependentValuesFeature(ConstraintCollection<?> columnStatsConstraintCollection) {
        featureName = DISTINCT_DEPENDENT_VALUES_FEATURE_NAME;

        // Initialize the distinct value counts.
        this.distinctValues = new Int2LongOpenHashMap((int) columnStatsConstraintCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof ColumnStatistics).count());
        columnStatsConstraintCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof ColumnStatistics)
                .map(constraint -> (ColumnStatistics) constraint)
                .forEach(distinctValueCount -> distinctValues.put(
                        distinctValueCount.getColumnId(),
                        distinctValueCount.getNumDistinctValues()
                ));
    }

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();

            long depDistinctValueCount = this.distinctValues.get(fkc.getDependentColumnId());
            instance.getFeatureVector().put(DISTINCT_DEPENDENT_VALUES_FEATURE_NAME, depDistinctValueCount);
        }
    }
}
