package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class CoverageFeature extends Feature {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static String COVERAGE_FEATURE_NAME = "Coverage";

    /**
     * Stores the distinct value counts for all columns.
     */
    private final Int2LongMap distinctValues;

    public CoverageFeature(ConstraintCollection columnStatsConstraintCollection) {
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

            double coverage = depDistinctValueCount/refDistinctValueCount;
            instance.getFeatureVector().put(COVERAGE_FEATURE_NAME, coverage);
        }
    }
}
