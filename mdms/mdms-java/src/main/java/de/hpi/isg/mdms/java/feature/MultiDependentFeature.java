package de.hpi.isg.mdms.java.feature;

import de.hpi.isg.mdms.java.util.Instance;
import de.hpi.isg.mdms.java.util.UnaryForeignKeyCandidate;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class MultiDependentFeature extends Feature {

//    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static String MULTI_DEPENDENT_FEATURE_NAME = "multi_dependent";

    /**
     * Stores the number of inds.
     */
    private int numINDs;

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        featureName = MULTI_DEPENDENT_FEATURE_NAME;
        featureType = FeatureType.Numeric;

        numINDs = instanceCollection.size();

        // Count the number of references for the columns.
        Int2IntOpenHashMap columnNumDependentOccurrences = new Int2IntOpenHashMap();
        columnNumDependentOccurrences.defaultReturnValue(0);
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            columnNumDependentOccurrences.addTo(depColumn, 1);
        }

        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            final int numDependentOccurrences = columnNumDependentOccurrences.get(depColumn);
            double normalized = normalize(numDependentOccurrences);
//            double normalized = numDependentOccurrences;
            instance.getFeatureVector().put(MULTI_DEPENDENT_FEATURE_NAME, normalized);
        }

    }

    @Override
    public double normalize(double valueForNormalizing) {
        return valueForNormalizing/(double)numINDs;
    }
}
